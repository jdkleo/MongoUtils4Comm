package com.odianyun.architecture.mongo.dto;

import java.io.Serializable;

/**
 * Created by zhangyang on 2016/10/24.
 */
public class MongoConnDTO implements Serializable{

    String host;
    String user;
    String pwd;
    String dbname;
    int minConn = 1;
    int maxConn = 1;

    public MongoConnDTO() {
    }

    public MongoConnDTO(String host, String user, String pwd, String dbname) {
        this.host = host;
        this.user = user;
        this.pwd = pwd;
        this.dbname = dbname;
    }

    public MongoConnDTO(String host, String user, String pwd, String dbname,int minConn,int maxConn) {
        this.host = host;
        this.user = user;
        this.pwd = pwd;
        this.dbname = dbname;
        this.minConn = minConn;
        this.maxConn = maxConn;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public String getDbname() {
        return dbname;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public int getMinConn() {
        return minConn;
    }

    public void setMinConn(int minConn) {
        this.minConn = minConn;
    }

    public int getMaxConn() {
        return maxConn;
    }

    public void setMaxConn(int maxConn) {
        this.maxConn = maxConn;
    }
}
