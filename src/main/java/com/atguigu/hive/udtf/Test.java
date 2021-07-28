package com.atguigu.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;

public class Test extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //约束函数的参数个数
        if(argOIs.getAllStructFieldRefs().size()!=1){
            throw new UDFArgumentLengthException("函数参数个数需为1");
        }
        //约束函数的参数类型
        String typeName = argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName();
        if ("string".equals(typeName)){
            throw new UDFArgumentTypeException(0,"参数类型需为string");
        }

        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("item");
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        //获取传入的数据
        String jsonArrStr = objects[0].toString();
        //将字符串转变成json数组
        JSONArray jsonArray = new JSONArray(jsonArrStr);
        //将jsonAray切分
        for (int i = 0; i < jsonArray.length(); i++) {
            String jsonStr = jsonArray.getString(i);
            //因为初始化方法里面限定了返回值类型是struct结构体
            //所以在这个地方不能直接输出jsonStr,需要用个字符串数组包装下
            String[] result = new String[1];
            result[0] = jsonStr;
            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
