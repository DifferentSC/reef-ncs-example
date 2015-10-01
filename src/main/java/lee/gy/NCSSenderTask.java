package lee.gy;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
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
import java.util.logging.Logger;

public class NCSSenderTask implements Task {

  @NamedParameter
  public static class SenderName implements Name<String> {
  }

  @NamedParameter
  public static class ReceiverName implements Name<String> {
  }

  private static final Logger LOG = Logger.getLogger(NCSSenderTask.class.getName());
  private final Connection<String> conn;

  private static class DoNothingEventHandler<String> implements EventHandler<Message<String>> {
    @Override
    public void onNext(final Message<String> message) {
    }
  }

  @Inject
  public NCSSenderTask(final NetworkConnectionService ncs,
                @Parameter(SenderName.class) final String senderName,
                @Parameter(ReceiverName.class) final String receiverName)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final IdentifierFactory idFac = injector.getNamedInstance(NetworkConnectionServiceIdFactory.class);
    final Identifier connId = idFac.getNewInstance("connection");
    final Identifier senderId = idFac.getNewInstance(senderName);
    final Identifier receiverId = idFac.getNewInstance(receiverName);
    ncs.registerConnectionFactory(connId, new StringCodec(), new DoNothingEventHandler<String>(),
        new DoNothingListener(), senderId);

    ConnectionFactory<String> connFac = ncs.getConnectionFactory(connId);
    conn = connFac.newConnection(receiverId);
  }

  @Override
  public byte[] call(final byte[] memento) throws InterruptedException, NetworkException {

    final String[] strings = {"Hi", "Hello", "Guten Tag", "Bon Jour"};
    conn.open();
    for(int i = 0; i < 10; i++) {
      conn.write(strings[i % 4]);
      Thread.sleep(10);
    }
    return null;
  }
}
