package org.intermine.bio.dataconversion;

public class UMLS {
	private String identifier;
	private String name;
	private String semanticLabel;
	private String semanticType;
	private String dbType;
	private String dbId;
	public UMLS() {
		
	}
	public UMLS(String identifier, String name, String semanticType,String semanticLabel,String dbType,String dbId) {
		super();
		this.identifier = identifier;
		this.name = name;
		this.semanticLabel = semanticLabel;
		this.semanticType = semanticType;
		this.dbType = dbType;
		this.dbId = dbId;
	}
	public String getSemanticLabel() {
		return semanticLabel;
	}
	public String getIdentifier() {
		return identifier;
	}
	public String getName() {
		return name;
	}
	public String getSemanticType() {
		return semanticType;
	}
	public String getDbType() {
		return dbType;
	}
	public String getDbId() {
		return dbId;
	}
	
}
