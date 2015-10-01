package lee.gy;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public final class NCSExampleDriver {

  private static final Logger LOG = Logger.getLogger(NCSExampleDriver.class.getName());
  private final EvaluatorRequestor requestor;
  private final NameServer nameServer;
  private final String senderName, receiverName;
  private final AtomicInteger submittedContext;
  private final AtomicInteger submittedTask;
  private ActiveContext senderContext;
  private final String driverHostAddress;

  @Inject
  private NCSExampleDriver(final EvaluatorRequestor requestor) throws UnknownHostException, InjectionException {
    this.requestor = requestor;
    LOG.log(Level.FINE, "NCSExampleDriver started");
    Injector injector = Tang.Factory.getTang().newInjector();
    this.nameServer = injector.getInstance(NameServer.class);
    this.submittedContext = new AtomicInteger();
    this.submittedContext.set(0);
    this.submittedTask = new AtomicInteger();
    this.submittedTask.set(0);
    this.senderName = "sender";
    this.receiverName = "receiver";
    this.driverHostAddress = Inet4Address.getLocalHost().getHostAddress();

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
      final Configuration contextConf;
      if (submittedContext.compareAndSet(0, 1)) {
        contextConf = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "context_0")
            .build();
      } else {
        contextConf = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "context_1")
            .build();
      }
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
        final Configuration partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "receiver_task")
            .set(TaskConfiguration.TASK, NCSReceiverTask.class)
            .build();
        final Configuration netConf = NameResolverConfiguration.CONF
            .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, driverHostAddress)
            .set(NameResolverConfiguration.NAME_SERVICE_PORT, NCSExampleDriver.this.nameServer.getPort())
            .build();
        final JavaConfigurationBuilder taskConfBuilder =
            Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf, netConf);
        taskConfBuilder.bindNamedParameter(NCSReceiverTask.ReceiverName.class, receiverName);
        final Configuration taskConf = taskConfBuilder.build();
        context.submitTask(taskConf);
      }
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      if (task.getId().equals("receiver_task")) {
        // receiver task is ready, submit sender task
        final Configuration partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "sender_task")
            .set(TaskConfiguration.TASK, NCSSenderTask.class)
            .build();
        final Configuration netConf = NameResolverConfiguration.CONF
            .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, driverHostAddress)
            .set(NameResolverConfiguration.NAME_SERVICE_PORT, NCSExampleDriver.this.nameServer.getPort())
            .build();
        final JavaConfigurationBuilder taskConfBuilder =
            Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf, netConf);
        taskConfBuilder.bindNamedParameter(NCSSenderTask.ReceiverName.class, receiverName);
        taskConfBuilder.bindNamedParameter(NCSSenderTask.SenderName.class, senderName);
        final Configuration taskConf = taskConfBuilder.build();
        senderContext.submitTask(taskConf);
      }
    }
  }

}
