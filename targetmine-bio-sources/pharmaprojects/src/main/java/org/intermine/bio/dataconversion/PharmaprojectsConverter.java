package org.intermine.bio.dataconversion;

/*
 * Copyright (C) 2002-2019 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import java.io.Reader;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.io.File;
import java.io.IOException;

import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.xml.full.Item;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * 
 * @author
 */
public class PharmaprojectsConverter extends BioFileConverter
{
    //
    private static final String DATASET_TITLE = "Add DataSet.title here";
    private static final String DATA_SOURCE_NAME = "Add DataSource.name here";

    /**
     * Constructor
     * @param writer the ItemWriter used to handle the resultant items
     * @param model the Model
     */
    public PharmaprojectsConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
    }
	private static Map<String, JsonToStr> propertyNames = new HashMap<String, JsonToStr>();

	static {
		propertyNames.put("identifier",new JsonToStr("drugPrimaryName"));
		propertyNames.put("overview", new JsonToStr("overview"));
		propertyNames.put("origin", new JsonToStr("origin"));
		propertyNames.put("icd9", new JsonToStr("drugIcd9","${icd9Id} ${name}"));
		propertyNames.put("icd10", new JsonToStr("drugIcd10","${icd10Id} ${name}"));
		propertyNames.put("preClinical", new JsonToStr("preClinical"));
		propertyNames.put("phaseI", new JsonToStr("phaseI"));
		propertyNames.put("phaseII", new JsonToStr("phaseII"));
		propertyNames.put("phaseIII", new JsonToStr("phaseIII"));
		propertyNames.put("mechanismsOfAction", new JsonToStr("mechanismsOfAction"));
		propertyNames.put("originator", new JsonToStr("originatorName"));
		propertyNames.put("therapeuticClasses", new JsonToStr("therapeuticClasses","${therapeuticClassName}(${therapeuticClassStatus})"));
		propertyNames.put("pharmacokinetics", new JsonToStr("pharmacokinetics","${model} ${parameter} ${unit}"));
		propertyNames.put("patents", new JsonToStr("patents","${patentNumber}"));
		propertyNames.put("marketing", new JsonToStr("marketing"));
		propertyNames.put("recordUrl",new JsonToStr( "recordUrl"));
	}

	public String createPharmaProject(JSONObject item) throws ObjectStoreException {
		Item project = createItem("PharmaProject");
		for (Entry<String, JsonToStr> entry : propertyNames.entrySet()) {
			String opt = entry.getValue().toString(item);
			if(opt!=null && opt.length() > 0) {
				project.setAttribute(entry.getKey(), opt);
			}
		}
		JSONArray meshTerms = item.optJSONArray("drugMeshTerms");
		if(meshTerms!=null) {
			for (int i = 0; i < meshTerms.length(); i++) {
				JSONObject jsonObject = meshTerms.getJSONObject(i);
				String meshId = jsonObject.getString("meshId");
				String meshTerm = createMeshTerm(meshId);
				project.addToCollection("meshTerms", meshTerm);
			}
		}
		store(project);
		return project.getIdentifier();
	}
	public void createPharmaProjectCompounds(JSONObject item,String pharmaProjectRefId) throws ObjectStoreException {
		Item compounds = createItem("PharmaProjectCompound");
		String identifier = item.getString("drugPrimaryName");
		compounds.setAttribute("identifier", "PharmaProject: " +identifier);
		compounds.setAttribute("originalId", identifier);
		compounds.setReference("pharmaProject", pharmaProjectRefId);
		String casNumbers = item.optString("casNumbers");
		if(casNumbers!=null) {
			compounds.setAttribute("casRegistryNumber", casNumbers);
		}
		JSONArray jsonArray = item.getJSONArray("chemicalStructure");
        for (int i = 0; i < jsonArray.length(); i++) {
        	String smiles = jsonArray.getString(i);
        	String inchiKey = smilesToInchiKeyMap.get(smiles);
        	if(inchiKey!=null) {
    			compounds.setAttribute("inchiKey", inchiKey);
        	}
        }
		store(compounds);
	}
	private HashMap<String,String> meshTermIds = new HashMap<String,String>();
	private String createMeshTerm(String meshId) throws ObjectStoreException {
		String meshTermRef = meshTermIds.get(meshId);
		if(meshTermRef!=null) {
			return meshTermRef;
		}
		Item meshItem = createItem("MeshTerm");
		meshItem.setAttribute("identifier", meshId);
		store(meshItem);
		meshTermRef = meshItem.getIdentifier();
		meshTermIds.put(meshId, meshTermRef);
		return meshTermRef;
	}
	private String readAll(Reader reader) throws IOException {
		StringBuilder sb = new StringBuilder();
		char[] buff = new char[4096];
		int len = 0;
		while((len = reader.read(buff))>0) {
			sb.append(buff, 0, len);
		}
		return sb.toString();
	}
	HashMap<String,String> smilesToInchiKeyMap = new HashMap<String,String>();
	private void loadSmilesToInchiKey() throws IOException {
		if(!smilesToInchiKeyMap.isEmpty()) {
			return;
		}
		Files.lines(smilesInchiKeyFile.toPath()).forEach(line ->{
			String[] split = line.split("\t");
			String smiles = split[0];
			String inchikey = split[1];
			smilesToInchiKeyMap.put(smiles,inchikey);
		});
		System.out.println("smilesInchiKeyFile loaded " + smilesToInchiKeyMap.size() +" entries");
	}
	private File smilesInchiKeyFile;
	public void setSmilesInchiKeyFile(File smilesInchiKeyFile) {

		this.smilesInchiKeyFile = smilesInchiKeyFile;
	}
    /**
     * 
     *
     * {@inheritDoc}
     */
    public void process(Reader reader) throws Exception {
    	loadSmilesToInchiKey();
        JSONObject jsonObject = new JSONObject(readAll(reader));
        JSONArray jsonArray = jsonObject.getJSONArray("items");
        for (int i = 0; i < jsonArray.length(); i++) {
        	JSONObject item = jsonArray.getJSONObject(i);
        	String pharmaProject = createPharmaProject(item);
        	createPharmaProjectCompounds(item, pharmaProject);
		}
    }
}
