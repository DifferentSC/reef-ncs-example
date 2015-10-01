package lee.gy;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

public class NCSExampleClient {
  private static final Logger LOG = Logger.getLogger(NCSExampleClient.class.getName());

  // Local runtime configuration
  private static Configuration getRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        // There are two tasks - sender & receiver
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, 3)
        .build();
  }

  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(NCSExampleDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "NCSExampleDriver")
        .set(DriverConfiguration.ON_DRIVER_STARTED, NCSExampleDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, NCSExampleDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, NCSExampleDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, NCSExampleDriver.RunningTaskHandler.class)
        .build();
  }

  public static void main(final String args[]) throws BindException, InjectionException {
    final Configuration runtimeConf = getRuntimeConfiguration();
    final Configuration driverConf = getDriverConfiguration();

    final LauncherStatus status = DriverLauncher
        .getLauncher(runtimeConf)
        .run(driverConf, 20000);
    LOG.log(Level.FINE, "REEF job completed: {0}", status);
  }

  private NCSExampleClient() {
  }
}
