package com.yinhai.yhdi.common;

import com.esotericsoftware.kryo.Kryo;
import com.yinhai.yhdi.increment.entity.FileIndex;
import com.yinhai.yhdi.increment.poto.SqlPoto;

import java.util.ArrayList;

//使用线程池化技术，可能出现ThreadLocal内存泄露
public class KyroUtil {
	private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			kryo.register(SqlPoto.class, 99);
            kryo.register(ArrayList.class, 96);
			kryo.register(FileIndex.class, 97);
			System.out.print("A kryo is initialized \n");
			return kryo;
		};
	};
	
	public static Kryo getKryo() {
		return kryos.get();
	}
}
