package temperature;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKeyPair implements WritableComparable<MyKeyPair>
{

    private int year;

    private int temperature;


    public int getTemperature()
    {
        return temperature;
    }

    public void setTemperature(int temperature)
    {
        this.temperature = temperature;
    }

    public int getYear()
    {
        return year;
    }

    public void setYear(int year)
    {
        this.year = year;
    }

    //重写三个方法
    @Override
    public int compareTo(MyKeyPair o)
    {
        int iRet = Integer.compare(year, o.getYear());
        if (iRet != 0)
            return iRet;
        return Integer.compare(temperature, o.getTemperature());
    }

    /**
     * 使用RPC协议读取二进制流，序列化过程
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(year);
        out.writeInt(temperature);
    }

    /**
     * 使用RPC协议读取二进制流，反序列化过程
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.year = in.readInt();
        this.temperature = in.readInt();
    }

    @Override
    public String toString()
    {
        return year + "\t" + temperature;
    }

    @Override
    public int hashCode()
    {
        return new Integer(year + temperature).hashCode();
    }
}
