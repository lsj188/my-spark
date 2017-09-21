package cn.lsj.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class FileTools {
    static Configuration conf = new Configuration();

    public static void delPath(String tempOutputPath) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(tempOutputPath);
        if (fs.exists(p)) {
            fs.delete(p, true);
        }
        fs.close();
    }

    public static void moveFile(String souPath, String tarPath)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(tarPath);
        if (fs.exists(p)) {
            fs.delete(p, true);
        }
        fs.rename(new Path(souPath), p);
        fs.close();
    }

    public static void copyFile(String souPath, String tarPath, String unixTimePath)
            throws IOException {
        long begtime = getUnixFileTime(unixTimePath);
        long endtime = Calendar.getInstance().getTimeInMillis();
        long maxtime = 0;
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] listStatus = fs.listStatus(new Path(souPath));
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.getModificationTime() > begtime && fileStatus.getModificationTime() < endtime) {
                //取得目标文件名称。
                String parth = fileStatus.getPath().toString();
                String fileName = parth.split(File.separator)[parth.split(File.separator).length - 1]
                        .toString();
                String tar = "";
                //取得目标文件url;
                if (tarPath.charAt(tarPath.length() - 1) + "" == File.separator) {
                    tar = tarPath + fileName;
                } else {
                    tar = tarPath + File.separator + fileName;
                }
                Path p = new Path(tar);
                if (fs.exists(p)) {
                    fs.delete(p, true);
                }
                //				fs.rename(new Path(souPath), p);
                //				fs.copyFromLocalFile(new Path(parth), p);
                FileUtil.copy(fs, new Path(parth), fs, p, false, conf);
                if (maxtime < fileStatus.getModificationTime()) {
                    maxtime = fileStatus.getModificationTime();
                }
            }

        }
        saveUnixFileTime(unixTimePath, maxtime);
        fs.close();
    }

    public static void copyFile(String souPath, String tarPath, String time, int i)
            throws IOException {

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] listStatus = fs.listStatus(new Path(souPath));
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.getPath().toString().contains(time)) {
                //取得目标文件名称。
                String parth = fileStatus.getPath().toString();
                String fileName = parth.split(File.separator)[parth.split(File.separator).length - 1]
                        .toString();
                String tar = "";
                //取得目标文件url;
                if (tarPath.charAt(tarPath.length() - 1) + "" == File.separator) {
                    tar = tarPath + fileName;
                } else {
                    tar = tarPath + File.separator + fileName;
                }
                Path p = new Path(tar);
                if (fs.exists(p)) {
                    fs.delete(p, true);
                }
                //				fs.rename(new Path(souPath), p);
                //				fs.copyFromLocalFile(new Path(parth), p);
                FileUtil.copy(fs, new Path(parth), fs, p, false, conf);
            }
        }
        fs.close();
    }

    public static long getUnixFileTime(String path) throws IOException {
        File file = new File(path);
        if (!file.exists() || file.isDirectory())
            throw new FileNotFoundException();
        @SuppressWarnings("resource")
        BufferedReader br = new BufferedReader(new FileReader(file));
        String temp = null;
        temp = br.readLine();
        return Long.parseLong(temp);
    }

    public static void saveUnixFileTime(String path, long time) throws IOException {
        File file = new File(path);
        if (!file.exists()) {
            file.createNewFile();
        }
        FileOutputStream out = new FileOutputStream(path);
        PrintStream p = new PrintStream(out);
        p.println(time);
    }

    public static boolean isExistDirectory(String souPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(souPath))) {
            return true;
        } else {
            return false;
        }

    }

    public static int isExistFile(String souPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        int i = 0;
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(souPath), true);
        if (files.hasNext()) {
            i = i + 1;
            while (files.hasNext()) {
                i++;
            }
            return i;
        }
        return i;

    }

    public static FileStatus[] FileList(String souPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] listStatus = fs.listStatus(new Path(souPath));
        for (FileStatus fileStatus : listStatus) {
            fileStatus.getModificationTime();
        }
        return listStatus;
    }

    public static void moveFiles(String souPath, String tarPath, String filePrefix) throws IOException {

        FileSystem fs = FileSystem.get(conf);

        //取得源文件列表
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(souPath), true);
        //判断目标文件夹是否存在，如果不存在就创建；
        Path fo = new Path(tarPath);
        if (!fs.exists(fo)) {
            fs.mkdirs(fo);
        }
        //循环读取源文件列表，拷贝到目标文件夹下面
        while (files.hasNext()) {
            String urlile = files.next().getPath().toString();
            //取得目标文件名称。
            String fileName = filePrefix + "_" + urlile.split(File.separator)[urlile.split(File.separator).length - 1]
                    .toString();
            String tar = "";
            //取得目标文件url;
            if (tarPath.charAt(tarPath.length() - 1) + "" == File.separator) {
                tar = tarPath + fileName;
            } else {
                tar = tarPath + File.separator + fileName;
            }
            //判断目标文件是否存在，是就删掉
            Path p = new Path(tar.replace("-", "_"));
            if (fs.exists(p)) {
                fs.delete(p, true);
            }
            System.out.println(tar);
            Pattern pp = Pattern.compile("SUCCESS");
            Matcher m = pp.matcher(tar);
            if (!m.find()) {
                System.out.println(p.toString());
                fs.rename(new Path(urlile), p);

            }


        }
        //关闭文件系统
        fs.close();
    }

    public static void moveFiles(String souPath, String tarPath) throws IOException {

        FileSystem fs = FileSystem.get(conf);

        //取得源文件列表
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(souPath), true);
        //判断目标文件夹是否存在，如果不存在就创建；
        Path fo = new Path(tarPath);
        if (!fs.exists(fo)) {
            fs.mkdirs(fo);
        }
        //循环读取源文件列表，拷贝到目标文件夹下面
        while (files.hasNext()) {
            String urlile = files.next().getPath().toString();
            //取得目标文件名称。
            String fileName = urlile.split(File.separator)[urlile.split(File.separator).length - 1]
                    .toString();
            String tar = "";
            //取得目标文件url;
            if (tarPath.charAt(tarPath.length() - 1) + "" == File.separator) {
                tar = tarPath + fileName;
            } else {
                tar = tarPath + File.separator + fileName;
            }
            //判断目标文件是否存在，是就删掉
            Path p = new Path(tar.replace("-", "_"));
            if (fs.exists(p)) {
                fs.delete(p, true);
            }
            System.out.println(tar);
            Pattern pp = Pattern.compile("SUCCESS");
            Matcher m = pp.matcher(tar);
            if (!m.find()) {
                System.out.println(p.toString());
                fs.rename(new Path(urlile), p);

            }


        }
        //关闭文件系统
        fs.close();
    }
}
