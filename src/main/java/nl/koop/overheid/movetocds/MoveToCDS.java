package nl.koop.overheid.movetocds;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.JobReport;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.DocumentMetadataHandle;
import nl.koop.overheid.movetocds.utils.Config;
import nl.koop.overheid.movetocds.utils.MarkLogicUtility;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.UUID;

public class MoveToCDS {
  private static final Logger LOG = LoggerFactory.getLogger(MoveToCDS.class);
  private static DataMovementManager manager;
  private static final String OPERA_COLLECTION = "/opera/options/%s";
  private static final String OPERA_OPTIONS_OPDRACHT_BESTANDEN = "/opera/options/opdrachtbestanden";

  public MoveToCDS() {
    this(Config.user, Config.password);
  }

  public MoveToCDS(String user, String password) {
    manager = MarkLogicUtility.getDatabaseClient(user, password).newDataMovementManager();
  }

  public boolean process(Path zipPayload, String oin, String idLevering, String verzoekFase) {
    LOG.debug("start processing");
    ServerTransform transform = new ServerTransform("projects-opera-opera-envelope");
    transform.put("oin", oin);
    transform.put("idlevering", idLevering);
    WriteBatcher batcher  = manager.newWriteBatcher()
      .withThreadCount(10)
      .withBatchSize(20)
      .withTransform(transform)
      .onBatchSuccess(batch-> {
        LOG.info("{} documents written: {}, JobBatchNr {}, JobTicketNr {}",
            batch.getTimestamp().getTime(),
            batch.getJobWritesSoFar(),
            batch.getJobBatchNumber(),
            batch.getJobTicket().getJobId()
        );
      })
      .onBatchFailure((batch,throwable) -> {
        LOG.error("Batch failed with {}", throwable.getLocalizedMessage());
        throwable.printStackTrace();
      });
    JobTicket ticket = manager.startJob(batcher);
    DocumentMetadataHandle metadata = new DocumentMetadataHandle();
    metadata.getCollections().addAll(
        String.format("/opera/job/%s", UUID.randomUUID()),
        String.format(OPERA_COLLECTION, verzoekFase.toLowerCase()),
        OPERA_OPTIONS_OPDRACHT_BESTANDEN,
        String.format("/opera/oin/%s", oin),
        String.format("/opera/idlevering/%s", idLevering)
    );
    DocumentMetadataHandle.DocumentMetadataValues metadataValues = metadata.getMetadataValues();
    metadataValues.put("job-id", ticket.getJobId());
    metadataValues.put("id-aanleveraar", oin);
    metadataValues.put("id-levering", idLevering);

    try (ZipFile zip = new ZipFile(zipPayload.toFile())) {
      Enumeration<ZipArchiveEntry> e = zip.getEntries();

      while(e.hasMoreElements()) {
        ZipArchiveEntry entry = e.nextElement();

        if (entry.isDirectory()) {
          continue;
        }
        String uri = String.format("/opera/import-%s/%s/%s/%s", verzoekFase.toLowerCase(), oin, idLevering, entry.getName());
        batcher.addAs(uri, metadata, zip.getInputStream(entry));
      }
      batcher.flushAndWait();
      manager.stopJob(ticket);
      batcher.awaitCompletion();
      JobReport report = manager.getJobReport(ticket);
      if (report.getFailureEventsCount() > 0) {
        LOG.info("BATCH ISSUES::" + String.format("WriteBatcher failed to write %s event(s)", report.getFailureEventsCount()));
      }
      LOG.info("Duration in seconds {}", (report.getJobEndTime().getTimeInMillis() - report.getJobStartTime().getTimeInMillis())/1000);
      return true;        
    } catch (IOException e) {
      LOG.debug("ZIP IO ISSUES::", e);
    }
    LOG.debug("stop processing");
    return true;
  }
  
}
