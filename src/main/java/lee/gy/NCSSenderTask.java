package lee.gy;

import com.google.inject.Inject;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.util.StringIdentifierFactory;
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
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.StringCodec;

import java.util.logging.Logger;

public class NCSSenderTask implements Task {

  @NamedParameter
  public static class SenderName implements Name<String> {
  }

  @NamedParameter
  public static class ReceiverName implements Name<String> {
  }

  private static Logger LOG = Logger.getLogger(NCSSenderTask.class.getName());
  private final Connection<String> conn;

  private static class DoNothingEventHandler<String> implements EventHandler<Message<String>> {
    @Override
    public void onNext(final Message<String> message) {
    }
  }

  @Inject
  NCSSenderTask(final NetworkConnectionService ncs,
                @Parameter(SenderName.class) final String senderName,
                @Parameter(ReceiverName.class) final String receiverName)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final IdentifierFactory idFac = injector.getInstance(StringIdentifierFactory.class);
    Identifier connId = idFac.getNewInstance("receiver_connection");
    Identifier senderId = idFac.getNewInstance(senderName);
    Identifier receiverId = idFac.getNewInstance(receiverName);
    Codec<String> codec = new StringCodec();
    ncs.registerConnectionFactory(connId, codec, new DoNothingEventHandler<String>(),
        new DoNothingListener(), senderId);

    ConnectionFactory<String> connFac = ncs.getConnectionFactory(connId);
    conn = connFac.newConnection(receiverId);
  }

  @Override
  public byte[] call(byte[] memento) throws InterruptedException {

    String[] strings = {"Hi", "Hello", "Guten Tag", "Bon Jour"};
    for(int i = 0; i < 10; i++) {
      conn.write(strings[i % 4]);
      Thread.sleep(10);
    }
    return null;
  }
}
