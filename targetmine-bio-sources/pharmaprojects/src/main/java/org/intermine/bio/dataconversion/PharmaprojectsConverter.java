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

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
	private static final Logger LOG = LogManager.getLogger(PharmaprojectsConverter.class);

    //
    private static final String DATASET_TITLE = "PharmaProjects";
    private static final String DATA_SOURCE_NAME = "PharmaProjects";

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
		propertyNames.put("overview", new JsonToStr("overview"));
		propertyNames.put("origin", new JsonToStr("origin"));
		propertyNames.put("icd9", new JsonToStr("drugIcd9","${icd9Id} ${name}"));
		propertyNames.put("icd10", new JsonToStr("drugIcd10","${icd10Id} ${name}"));
		propertyNames.put("preClinical", new JsonToStr("preClinical"));
		propertyNames.put("phaseI", new JsonToStr("phaseI"));
		propertyNames.put("phaseII", new JsonToStr("phaseII"));
		propertyNames.put("phaseIII", new JsonToStr("phaseIII"));
		propertyNames.put("mechanismsOfAction", new JsonToStr("mechanismsOfAction","${directMechanism}"));
		propertyNames.put("originator", new JsonToStr("originatorName"));
		propertyNames.put("therapeuticClasses", new JsonToStr("therapeuticClasses","${therapeuticClassName}(${therapeuticClassStatus})"));
		propertyNames.put("pharmacokinetics", new JsonToStr("pharmacokinetics","${model} ${parameter} ${unit}"));
		propertyNames.put("patents", new JsonToStr("patents","${patentNumber}"));
		propertyNames.put("marketing", new JsonToStr("marketing"));
		propertyNames.put("recordUrl",new JsonToStr( "recordUrl"));
	}

	public String createPharmaProject(JSONObject item) throws ObjectStoreException {
		Item project = createItem("PharmaProject");
		String identifier = item.getString("drugId");
		project.setAttribute("identifier", identifier);
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
		JSONArray jsonArray = item.getJSONArray("targets");
        for (int i = 0; i < jsonArray.length(); i++) {
        	JSONObject targets = jsonArray.getJSONObject(i);
        	Object opt = targets.opt("entrezGeneId");
        	if(opt != null) {
        		String gene = getGene(opt.toString());
        		project.addToCollection("targets", gene);
        	}
        }
		jsonArray = item.getJSONArray("trialIds");
        for (int i = 0; i < jsonArray.length(); i++) {
        	int trialId = jsonArray.getInt(i);
        	String trialTroveRef = createTrialTrove(trialId);
        	project.addToCollection("trials", trialTroveRef);
        }
		store(project);
		return project.getIdentifier();
	}
	HashMap<Integer, String> trialMap = new HashMap<Integer, String>();
	private String createTrialTrove(Integer trialId) throws ObjectStoreException {
		String trialRefId = trialMap.get(trialId);
		if(trialRefId!=null) {
			return trialRefId;
		}
		Item project = createItem("TrialTrove");
		project.setAttribute("name", "TrialTroveID-" + trialId);
		store(project);
		trialMap.put(trialId,project.getIdentifier());
		return project.getIdentifier();
	}
	public void createPharmaProjectCompounds(JSONObject item,String pharmaProjectRefId) throws ObjectStoreException {
		Item compounds = createItem("PharmaProjectCompound");
		String identifier = item.getString("drugId");
		String name = item.getString("drugPrimaryName");
		String origin = item.optString("origin");
		compounds.setAttribute("identifier", "PharmaProject: " +identifier);
		compounds.setAttribute("originalId", name);
		if(origin!=null && origin.length() > 0){
			compounds.setAttribute("origin", origin);
		}
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
    			int indexof = inchiKey.indexOf("-");
    			if( 0 <= indexof ){
    				String compoundGroupId = inchiKey.substring(0, indexof );
    				if (compoundGroupId.length() == 14) {
    					String compoundGroupRef = getCompoundGroup(compoundGroupId, name);
    					compounds.setReference("compoundGroup", compoundGroupRef);
    				} else {
    					LOG.error(String.format("Bad InChIKey value: %s .", inchiKey));
    				}
    			}

        	}
    		Item structureItem = createItem( "CompoundStructure" );
    		structureItem.setAttribute( "type", "SMILES" );
    		structureItem.setAttribute( "value", smiles );
    		structureItem.setReference("compound", compounds);
    		store(structureItem);
        }
		jsonArray = item.getJSONArray("drugNameSynonyms");
        for (int i = 0; i < jsonArray.length(); i++) {
        	String synonyms = jsonArray.getString(i);
    		Item syn = createItem("CompoundSynonym");
    		syn.setAttribute("value", synonyms);
    		syn.setReference("subject", compounds);
    		store(syn);
        }

		store(compounds);
	}
    private Map<String, String> geneMap = new HashMap<String, String>();

    private String getGene(String primaryIdentifier) throws ObjectStoreException {
        if (StringUtils.isEmpty(primaryIdentifier)) {
            return "";
        }
        String ret = geneMap.get(primaryIdentifier);

        if (ret == null) {
            Item item = createItem("Gene");
            item.setAttribute("primaryIdentifier", primaryIdentifier);
            item.setAttribute("ncbiGeneId", primaryIdentifier);
            store(item);
            ret = item.getIdentifier();
            geneMap.put(primaryIdentifier, ret);
        }
        return ret;
    }

	// key is inchikey, value is compoundGroup Item's identifier
	private Map<String, String> compoundGroupMap = new HashMap<String, String>();

	private String getCompoundGroup(String inchiKey, String name) throws ObjectStoreException {
		String ret = compoundGroupMap.get(inchiKey);
		if (ret == null) {
			Item item = createItem("CompoundGroup");
			item.setAttribute("identifier", inchiKey);
			if( null != name && !"".equals( name ) ){
				item.setAttribute("name", name);
			}
			store(item);
			ret = item.getIdentifier();
			compoundGroupMap.put(inchiKey, ret);
		}
		return ret;
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
			if( split.length == 2){
				String smiles = split[0];
				String inchikey = split[1];
				smilesToInchiKeyMap.put(smiles,inchikey);
			}else{
				LOG.warn("Unexpected line " + line);
			}
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
