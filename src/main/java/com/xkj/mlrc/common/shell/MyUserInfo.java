package com.xkj.mlrc.common.shell;

/**
 * @author lijf@2345.com
 * @date 2020/4/24 14:56
 * @desc
 */

import com.jcraft.jsch.UserInfo;

public class MyUserInfo implements UserInfo {

    @Override
    public String getPassphrase() {
        // TODO Auto-generated method stub
        System.out.println("MyUserInfo.getPassphrase()");
        return null;
    }

    @Override
    public String getPassword() {
        // TODO Auto-generated method stub
        System.out.println("MyUserInfo.getPassword()");
        return null;
    }

    @Override
    public boolean promptPassphrase(String arg0) {
        // TODO Auto-generated method stub
        System.out.println("MyUserInfo.promptPassphrase()");
        System.out.println(arg0);
        return false;
    }

    @Override
    public boolean promptPassword(String arg0) {
        // TODO Auto-generated method stub
        System.out.println("MyUserInfo.promptPassword()");
        System.out.println(arg0);
        return false;
    }

    @Override
    public boolean promptYesNo(String arg0) {
        // TODO Auto-generated method stub'
        System.out.println("MyUserInfo.promptYesNo()");
        System.out.println(arg0);
        if (arg0.contains("The authenticity of host")) {
            return true;
        }
        return true;
    }

    @Override
    public void showMessage(String arg0) {
        // TODO Auto-generated method stub
        System.out.println("MyUserInfo.showMessage()");
    }

}