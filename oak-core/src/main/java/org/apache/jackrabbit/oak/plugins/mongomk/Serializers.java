package org.apache.jackrabbit.oak.plugins.mongomk;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.mongodb.BasicDBObject;

import java.util.ArrayList;
import java.util.Map;

public class Serializers {
    public static final Class[] PROPERTIES_GENERIC = new Class[]{String.class, String.class};

    public static Serializer<Node> NODE = new Serializer<Node>() {

        @Override
        public void write(Kryo kryo, Output output, Node n) {
            output.writeString(n.rev.toString());
            output.writeString(n.path);

            String lastRev = (n.lastRevision != null) ? n.lastRevision.toString() : null;
            output.writeString(lastRev);


            kryo.writeClass(output,n.properties.getClass());
            MapSerializer ms = createSerializer(kryo);
            ms.write(kryo, output, n.properties);
        }

        @Override
        public Node read(Kryo kryo, Input input, Class<Node> type) {
            Revision rev = Revision.fromString(input.readString());
            String path = input.readString();
            Node n = new Node(path,rev);

            String lastRev = input.readString();
            if(lastRev != null){
                n.setLastRevision(Revision.fromString(lastRev));
            }

            Class mapClass = kryo.readClass(input).getType();
            MapSerializer ms = createSerializer(kryo);
            n.properties.putAll(ms.read(kryo,input,mapClass));
            return n;
        }

        private MapSerializer createSerializer(Kryo kryo) {
            MapSerializer ms = new MapSerializer();
            ms.setKeysCanBeNull(false);
            ms.setGenerics(kryo, PROPERTIES_GENERIC);
            return ms;
        }
    };


    public static Serializer<Node.Children> CHILDREN = new Serializer<Node.Children>() {
        @Override
        public void write(Kryo kryo, Output output, Node.Children c) {
            output.writeBoolean(c.hasMore);

            CollectionSerializer cs = createSerializer(kryo);
            cs.write(kryo, output, c.children);
        }

        @Override
        public Node.Children read(Kryo kryo, Input input, Class<Node.Children> type) {
            Node.Children c = new Node.Children();
            c.hasMore = input.readBoolean();

            CollectionSerializer cs = createSerializer(kryo);
            //the read method generic type is not correct. So assign it via local var
            Class clazz = ArrayList.class;
            c.children.addAll(cs.read(kryo,input,clazz));
            return c;
        }

        private CollectionSerializer createSerializer(Kryo kryo) {
            CollectionSerializer cs = new CollectionSerializer();
            cs.setGenerics(kryo,new Class[] {String.class});
            cs.setElementsCanBeNull(false);
            return cs;
        }
    };

    public static Serializer<MongoDocumentStore.CachedDocument> DOCUMENTS = new Serializer<MongoDocumentStore.CachedDocument>() {
        @Override
        public void write(Kryo kryo, Output output, MongoDocumentStore.CachedDocument d) {
            output.writeLong(d.time);

            //Value can be null so need to handle it accordingly
            Map<String, Object> value = d.value;
            if(value != null){
                output.writeBoolean(true);
                kryo.writeClass(output,d.value.getClass());

                MapSerializer ms = createSerializer(kryo);
                ms.write(kryo,output,d.value);
            }else{
                output.writeBoolean(false);
            }
        }

        @Override
        public MongoDocumentStore.CachedDocument read(Kryo kryo, Input input, Class<MongoDocumentStore.CachedDocument> type) {
            long time = input.readLong();

            MongoDocumentStore.CachedDocument d = null;
            boolean valueNotNull = input.readBoolean();
            if(valueNotNull){
                Class mapType = kryo.readClass(input).getType();

                MapSerializer ms = createSerializer(kryo);
                Map<String,Object> data = ms.read(kryo,input,mapType);
                d = new MongoDocumentStore.CachedDocument(data,time);
            }else{
                d = new MongoDocumentStore.CachedDocument(null,time);
            }
            return d;
        }

        private MapSerializer createSerializer(Kryo kryo) {
            MapSerializer ms = new MapSerializer();
            ms.setKeysCanBeNull(false);
            ms.setKeyClass(String.class,kryo.getSerializer(String.class));
            return ms;
        }
    };

    public static Serializer BASIC_DB_OBJECT = new MapSerializer(){
        @Override
        public void write(Kryo kryo, Output output, Map map) {
            output.writeBoolean(((BasicDBObject)map).isPartialObject());
            super.write(kryo, output, map);
        }

        @Override
        protected Map create(Kryo kryo, Input input, Class<Map> type) {
            BasicDBObject bdo = new BasicDBObject();
            if(input.readBoolean()){
                bdo.markAsPartialObject();
            }
            return bdo;
        }

        @Override
        protected Map createCopy(Kryo kryo, Map original) {
            BasicDBObject bdo = new BasicDBObject();
            if(original instanceof BasicDBObject && ((BasicDBObject) original).isPartialObject()){
                bdo.markAsPartialObject();
            }
            return bdo;
        }
    };
}
