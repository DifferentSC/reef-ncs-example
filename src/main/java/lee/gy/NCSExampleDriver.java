package lee.gy;

import com.google.inject.Inject;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.reflections.Configuration;

import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public class NCSExampleDriver {

  private static final Logger LOG = Logger.getLogger(NCSExampleDriver.class.getName());
  private final EvaluatorRequestor requestor;
  private NameServer nameServer;

  @Inject
  NCSExampleDriver(final EvaluatorRequestor requestor) throws InjectionException {
    this.requestor = requestor;
    LOG.log(Level.FINE, "NCSExampleDriver started");
    Injector injector = Tang.Factory.getTang().newInjector();
    nameServer = injector.getInstance(NameServer.class);
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
      final Configuration contextConf = ContextConfiguration.
    }
  }

}
