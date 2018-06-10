package BatchAnalysis;

import java.io.Serializable;
import java.util.Date;



@SuppressWarnings("serial")
public class SearchEntry implements Serializable{
	
	private String userid;
	private String keywords; //isws xreiastei na ginei lista
	private Date date;
	private int pos;
	private String url;
	
	public SearchEntry() {
		super();
		//keywords = new ArrayList<String>();
	}
	
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getKeywords() {
		return keywords;
	}
	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date =  dateFormat.parse(date);
	}
	public int getPos() {
		return pos;
	}
	public void setPos(int pos) {
		this.pos = pos;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}

}
