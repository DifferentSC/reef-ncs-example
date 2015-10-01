package lee.gy;

import com.google.inject.Inject;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.StringCodec;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NCSReceiverTask implements Task {

  private static Logger LOG = Logger.getLogger(NCSReceiverTask.class.getName());

  private class StringMessageHandler implements EventHandler<Message<String>> {
    @Override
    public void onNext(final Message<String> message) {
      Iterator<String> iter = message.getData().iterator();
      while(!iter.hasNext()) {
        System.out.println("Message: " + iter.next());
      }
    }
  }

  @Inject
  NCSReceiverTask(final NetworkConnectionService ncs, final Identifier receiverId)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final IdentifierFactory idFac = injector.getInstance(StringIdentifierFactory.class);
    Identifier connId = idFac.getNewInstance("receiver_connection");
    ncs.registerConnectionFactory(connId, new StringCodec(), new StringMessageHandler(), new DoNothingListener()
    , receiverId);
    LOG.log(Level.FINE, "Receiver Task Started");
  }

  @Override
  public byte[] call(byte[] memento) throws InterruptedException {
    Thread.sleep(10000);
    return null;
  }
}
