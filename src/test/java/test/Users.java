package test;

/**
 * Users entity.
 * 
 * @author MyEclipse Persistence Tools
 */

public class Users implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	// Fields
	private Long id;
	private String name;
	private String goodsnum;
	private String password;
	private Long servergroup;
	private Long corpid;
	private String realname;
	private String tel;
	private Long state;

	// Constructors

	/** default constructor */
	public Users() {
	}

	/** full constructor */
	public Users(String name, String password, Long servergroup, Long corpid,
			String realname, String tel, Long state) {
		this.name = name;
		this.password = password;
		this.servergroup = servergroup;
		this.corpid = corpid;
		this.realname = realname;
		this.tel = tel;
		this.state = state;
	}

	// Property accessors

	public Long getId() {
		return this.id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPassword() {
		return this.password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Long getServergroup() {
		return this.servergroup;
	}

	public void setServergroup(Long servergroup) {
		this.servergroup = servergroup;
	}

	public Long getCorpid() {
		return this.corpid;
	}

	public void setCorpid(Long corpid) {
		this.corpid = corpid;
	}

	public String getRealname() {
		return this.realname;
	}

	public void setRealname(String realname) {
		this.realname = realname;
	}

	public String getTel() {
		return this.tel;
	}

	public void setTel(String tel) {
		this.tel = tel;
	}

	public Long getState() {
		return this.state;
	}

	public void setState(Long state) {
		this.state = state;
	}

	public String getGoodsnum() {
		return goodsnum;
	}

	public void setGoodsnum(String goodsnum) {
		this.goodsnum = goodsnum;
	}

}