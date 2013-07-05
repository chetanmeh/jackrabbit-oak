package org.apache.jackrabbit.oak.cache;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class ForwardingListener<K,V> implements RemovalListener<K,V>{
    private RemovalListener<K,V> delegate;

    @Override
    public void onRemoval(RemovalNotification<K, V> notification) {
        if(delegate != null){
            delegate.onRemoval(notification);
        }
    }

    public void setDelegate(RemovalListener<K, V> delegate) {
        this.delegate = delegate;
    }
}
