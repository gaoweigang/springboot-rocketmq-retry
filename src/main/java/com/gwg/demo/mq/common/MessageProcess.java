package com.gwg.demo.mq.common;


import com.gwg.demo.mq.common.DetailRes;

public interface MessageProcess<T> {
	
    public DetailRes process(T messageBean);
}

