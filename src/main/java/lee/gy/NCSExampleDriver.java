package lee.gy;

import com.google.inject.Inject;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.time.event.StartTime;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public class NCSExampleDriver {

  private static final Logger LOG = Logger.getLogger(NCSExampleDriver.class.getName());
  private final EvaluatorRequestor requestor;
  private final NameServer nameServer;
  private final String senderName, receiverName;
  private final AtomicInteger submittedTask;
  private ActiveContext senderContext;
  private String driverHostAddress;

  @Inject
  NCSExampleDriver(final EvaluatorRequestor requestor) throws InjectionException {
    this.requestor = requestor;
    LOG.log(Level.FINE, "NCSExampleDriver started");
    Injector injector = Tang.Factory.getTang().newInjector();
    this.nameServer = injector.getInstance(NameServer.class);
    this.submittedTask = new AtomicInteger();
    this.submittedTask.set(0);
    this.senderName = "sender";
    this.receiverName = "receiver";

    try {
      this.driverHostAddress = Inet4Address.getLocalHost().getHostAddress();
    } catch(UnknownHostException e) {
      LOG.log(Level.SEVERE, "Cannot get driver host address! " + e.toString());
      this.driverHostAddress = "127.0.0.1";
    }
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      NCSExampleDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(2)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
      LOG.log(Level.FINE, "Requested Evaluator");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator evaluator) {
      LOG.log(Level.FINE, "Evaluator allocated");
      Configuration contextConf = ContextConfiguration.CONF.build();
      evaluator.submitContext(contextConf);
    }
  }

  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      if (submittedTask.compareAndSet(0, 1)) {
        // keep context for the sender task
        senderContext = context;
      } else {
        Configuration partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "receiver_task")
            .build();
        Configuration netConf = NameResolverConfiguration.CONF
            .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, driverHostAddress)
            .set(NameResolverConfiguration.NAME_SERVICE_PORT, NCSExampleDriver.this.nameServer.getPort())
            .build();
        JavaConfigurationBuilder taskConfBuilder =
            Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf, netConf);
        taskConfBuilder.bindNamedParameter(NCSReceiverTask.ReceiverName.class, receiverName);
        Configuration taskConf = taskConfBuilder.build();
        context.submitTask(taskConf);
      }
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(RunningTask task) {
      if (task.getId().equals("receiver_task")) {
        // receiver task is ready, submit sender task
        Configuration partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "sender_task")
            .build();
        Configuration netConf = NameResolverConfiguration.CONF
            .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, driverHostAddress)
            .set(NameResolverConfiguration.NAME_SERVICE_PORT, NCSExampleDriver.this.nameServer.getPort())
            .build();
        JavaConfigurationBuilder taskConfBuilder =
            Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf, netConf);
        taskConfBuilder.bindNamedParameter(NCSSenderTask.ReceiverName.class, receiverName);
        taskConfBuilder.bindNamedParameter(NCSSenderTask.SenderName.class, senderName);
        Configuration taskConf = taskConfBuilder.build();
        senderContext.submitTask(taskConf);
      }
    }
  }

}
