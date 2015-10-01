package lee.gy;

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.net.SocketAddress;

public class DoNothingListener implements LinkListener<Message<String>> {
  @Override
  public void onSuccess(final Message<String> message) {
  }
  @Override
  public void onException(final Throwable cause, final SocketAddress addr, final Message<String> message) {
    throw new RuntimeException();
  }
}
