package fi.hsl;

import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.concurrent.ScheduledExecutorService;

@SpringBootApplication
@Slf4j
public class TransitdataSourceApplication {
    private static final int SQL_SERVER_ERROR_DEADLOCK_VICTIM = 1205;

    public static void main(String[] args) {
        log.info("Starting transitdata sources");
        SpringApplication.run(TransitdataSourceApplication.class, args);
/*        try {
            String table = ConfigUtils.getEnvOrThrow("PT_TABLE");
            PubtransTableType type = PubtransTableType.fromString(table);

            Config config = null;
            if (type == PubtransTableType.ROI_ARRIVAL) {
                config = ConfigParser.createConfig("arrival.conf");
            } else if (type == PubtransTableType.ROI_DEPARTURE) {
                config = ConfigParser.createConfig("departure.conf");
            } else {
                log.error("Failed to get table name from PT_TABLE-env variable, exiting application");
                System.exit(1);
            }

            Connection connection = createPubtransConnection();

            final PulsarApplication app = PulsarApplication.newInstance(config);
            PulsarApplicationContext context = app.getContext();

            final PubtransConnector connector = PubtransConnector.newInstance(connection, context, type);

            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            log.info("Starting scheduler");

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    if (connector.checkPrecondition()) {
                        connector.queryAndProcessResults();
                    } else {
                        log.error("Pubtrans poller precondition failed, skipping the current poll cycle.");
                    }
                } catch (SQLServerException sqlServerException) {
                    // Occasionally (once every 2 hours?) the driver throws us out as a deadlock victim.
                    // There's no easy way to fix the root problem so lets just convert it to warning.
                    // More info: https://stackoverflow.com/questions/8390322/cause-of-a-process-being-a-deadlock-victim
                    if (sqlServerException.getErrorCode() == SQL_SERVER_ERROR_DEADLOCK_VICTIM) {
                        log.warn("SQL Server evicted us as deadlock victim. ignoring this for now...", sqlServerException);
                    } else {
                        log.error("SQL Server Unexpected error code, shutting down", sqlServerException);
                        log.warn("Driver Error code: {}", sqlServerException.getErrorCode());
                        closeApplication(app, scheduler);
                    }
                } catch (JedisException | SQLException | PulsarClientException connectionException) {
                    log.error("Connection problem, cannot recover so shutting down", connectionException);
                    closeApplication(app, scheduler);
                } catch (Exception e) {
                    log.error("Unknown error at Pubtrans scheduler, shutting down", e);
                    closeApplication(app, scheduler);
                }
            }, 0, 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Exception at Main", e);
        }*/
    }

    private static void closeApplication(PulsarApplication app, ScheduledExecutorService scheduler) {
        log.warn("Closing application");
        scheduler.shutdown();
        app.close();
    }

    private static Connection createPubtransConnection() throws Exception {
        String connectionString = "";
        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String secretFilePath = ConfigUtils.getEnv("FILEPATH_CONNECTION_STRING").orElse("/run/secrets/pubtrans_community_conn_string");
            connectionString = new Scanner(new File(secretFilePath))
                    .useDelimiter("\\Z").next();
        } catch (Exception e) {
            log.error("Failed to read Pubtrans connection string from secrets", e);
            throw e;
        }

        if (connectionString.isEmpty()) {
            throw new Exception("Failed to find Pubtrans connection string, exiting application");
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionString);
            log.info("Database connection created");
        } catch (SQLException e) {
            log.error("Failed to connect to Pubtrans database", e);
            throw e;
        }

        return connection;
    }
}
