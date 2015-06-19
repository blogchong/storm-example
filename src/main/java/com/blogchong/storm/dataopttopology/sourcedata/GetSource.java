package com.blogchong.storm.dataopttopology.sourcedata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

/**
 * @author blogchong
 * @version 2015年06月07日 上午14:31:25
 * @Blog www.blogchong.com
 * @米特吧大数据论坛　www.mite8.com
 * @email blogchong@163.com
 * @QQ_G 191321336
 * @Weixin: blogchong
 * @Des 构造一个随机的域名交易信息源，写入文件中
 */

//该的目的是构造一个随机的domain数据集
public class GetSource {

    public static void main(String[] args) {

        Random random = new Random();

        int note_num = 10000;

        //构造一个随机记录
        String[] net0 = {"baidu", "hitwh", "google", "jikexueyuan", "hadoop",
                "storm", "blogchong", "cooje", "mite8", "mygod"};
        String[] net1 = {"com", "net", "cn", "edu", "tv", "org", "us", "jp","club" ,"pub",
                "rec", "info"};
        String[] times = {"2000", "2001", "2002", "2005", "2007", "2010",
                "2011", "2012", "2013", "1998", "2014", "2015"};
        String[] value = {"1326", "1446", "1401", "1202", "1871", "2000",
                "122", "23000", "400", "240", "8888", "100000", "34123"};
        String[] validity = {"3", "5", "20", "100", "32", "12", "50", "1",
                "23", "45", "200"};
        String[] seller = {"Huang", "Lina", "James", "Gale", "Kathryn",
                "Acong", "Green", "Facke", "Nina", "Litao", "Pony", "Blogchong", "Mite"};

        // 写入中文字符时解决中文乱码问题
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(new File("domain.log"));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }
        OutputStreamWriter osw = null;
        try {
            osw = new OutputStreamWriter(fos, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        BufferedWriter bw = new BufferedWriter(osw);

        for (int i = 0; i < note_num; i++) {
            // 构造域名
            String net = "www." + net0[random.nextInt(net0.length)] + "."
                    + net1[random.nextInt(net1.length)];
            String records = net + "\t" + value[random.nextInt(value.length)] + "\t"
                    + times[random.nextInt(times.length)] + "\t"
                    + validity[random.nextInt(validity.length)] + "\t"
                    + seller[random.nextInt(seller.length)];

            try {
                bw.write(records);
                bw.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 注意关闭的先后顺序，先打开的后关闭，后打开的先关闭
        try {
            bw.close();
            osw.close();
            fos.close();
            System.out.println("write ok !");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
