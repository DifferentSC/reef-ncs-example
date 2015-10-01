package lee.gy;

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.impl.config.NetworkConnectionServiceIdFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.impl.StringCodec;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NCSReceiverTask implements Task {

  @NamedParameter
  public static class ReceiverName implements Name<String> {
  }

  private static final Logger LOG = Logger.getLogger(NCSReceiverTask.class.getName());

  private class StringMessageHandler implements EventHandler<Message<String>> {
    @Override
    public void onNext(final Message<String> message) {
      final Iterator<String> iter = message.getData().iterator();
      while(iter.hasNext()) {
        System.out.println("Message: " + iter.next());
      }
    }
  }

  @Inject
  public NCSReceiverTask(final NetworkConnectionService ncs,
                  final @Parameter(ReceiverName.class) String receiverName)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final IdentifierFactory idFac = injector.getNamedInstance(NetworkConnectionServiceIdFactory.class);
    final Identifier connId = idFac.getNewInstance("connection");
    final Identifier receiverId = idFac.getNewInstance(receiverName);
    ncs.registerConnectionFactory(connId, new StringCodec(), new StringMessageHandler(), new DoNothingListener()
    , receiverId);
    LOG.log(Level.FINE, "Receiver Task Started");
  }

  @Override
  public byte[] call(final byte[] memento) throws InterruptedException {
    // Spin wait
    while(true);
  }
}
