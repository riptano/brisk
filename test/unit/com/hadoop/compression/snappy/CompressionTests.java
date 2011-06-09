package com.hadoop.compression.snappy;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

public class CompressionTests
{
    @Test
    public void testSnappyCompression() throws IOException
    {

        SnappyCodec c = new SnappyCodec(new Configuration());
        byte[] inmsg = new byte[1024 * 1024 * 10];
        fillArray(inmsg);
        
        byte[] buffer = new byte[1024 * 1024];
        byte[] outmsg = new byte[1024 * 1024 * 16];

        for (int k = 0; k < 64; k++)
        {

            ByteArrayOutputStream bout = new ByteArrayOutputStream();

            CompressionOutputStream cout = c.createOutputStream(bout);

           
            cout.write(inmsg);
            cout.flush();

            ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
            CompressionInputStream cin = c.createInputStream(bin);

          
            int totaln = 0;

            while (cin.available() > 0)
            {
                int n = cin.read(buffer);
                if (n < 0)
                    break;

                try
                {
                    System.arraycopy(buffer, 0, outmsg, totaln, n);

                }
                catch (Throwable t)
                {
                    System.err.println("n = " + n + " totaln " + totaln);
                    throw new RuntimeException(t);
                }
                totaln += n;
            }

            assertEquals(inmsg.length, totaln);

            for (int i = 0; i < inmsg.length; i++)
            {
                assertEquals(inmsg[i], outmsg[i]);
            }

            assertEquals(new String(inmsg), new String(outmsg, 0, totaln));

        }

    }

    private void fillArray(byte[] buf)
    {
        for (int j = 0; j < buf.length; j++)
        {
            buf[j] = (byte) j;
        }
    }

}
