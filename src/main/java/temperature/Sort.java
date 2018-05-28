package temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Sort extends WritableComparator
{
    public Sort()
    {
        super(MyKeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b)
    {
        MyKeyPair k1 = (MyKeyPair) a;
        MyKeyPair k2 = (MyKeyPair) b;

        int iRet = Integer.compare(k1.getYear(), k2.getYear());
        if (iRet != 0)
            return iRet;
        return Integer.compare(k2.getTemperature(), k1.getTemperature());

    }
}
