package com.zillionfortune.realtime.model;

public class Ywlog {
	private String uuid;//非登录用户的uuid
	private String  mobile; //手机号码
	private String vd;//设备号
	private String os;//系统版本
	private String platform;//平台版本
	private String web; //浏览器版本
	private String vd_wh;//设备长宽
	private String channelCode;//渠道号
	private String appVd;//app版本号
	private String mobileType;//机型
	private String actionType;//点击行为编号
	private String messageContent;//消息内容
	private String messageType;//消息类型
	private String ip;//ip
	private String logtime; //日志时间
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	public String getMobile() {
		return mobile;
	}
	public void setMobile(String mobile) {
		this.mobile = mobile;
	}
	public String getVd() {
		return vd;
	}
	public void setVd(String vd) {
		this.vd = vd;
	}
	public String getOs() {
		return os;
	}
	public void setOs(String os) {
		this.os = os;
	}
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getWeb() {
		return web;
	}
	public void setWeb(String web) {
		this.web = web;
	}
	public String getVd_wh() {
		return vd_wh;
	}
	public void setVd_wh(String vd_wh) {
		this.vd_wh = vd_wh;
	}
	public String getChannelCode() {
		return channelCode;
	}
	public void setChannelCode(String channelCode) {
		this.channelCode = channelCode;
	}
	public String getAppVd() {
		return appVd;
	}
	public void setAppVd(String appVd) {
		this.appVd = appVd;
	}
	public String getMobileType() {
		return mobileType;
	}
	public void setMobileType(String mobileType) {
		this.mobileType = mobileType;
	}
	public String getActionType() {
		return actionType;
	}
	public void setActionType(String actionType) {
		this.actionType = actionType;
	}
	public String getMessageContent() {
		return messageContent;
	}
	public void setMessageContent(String messageContent) {
		this.messageContent = messageContent;
	}
	public String getMessageType() {
		return messageType;
	}
	public void setMessageType(String messageType) {
		this.messageType = messageType;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getLogtime() {
		return logtime;
	}
	public void setLogtime(String logtime) {
		this.logtime = logtime;
	}
	
}
