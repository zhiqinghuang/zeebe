package io.zeebe.distributedlog;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.nio.ByteBuffer;

public class ByteBufferSerializer extends Serializer<ByteBuffer> {

  @Override
  public void write(Kryo kryo, Output output, ByteBuffer object) {
  /*  byte[] ;
    object.get(output.getBuffer(), output.position(), object.capacity());

    //
    if (object == null) {
      output.writeVarInt(NULL, true);
      return;
    }
    output.writeVarInt(object.capacity() + 1, true);
    output.writeBytes(object);*/
  }

  @Override
  public ByteBuffer read(Kryo kryo, Input input, Class<ByteBuffer> type) {
    return null;
  }
}
