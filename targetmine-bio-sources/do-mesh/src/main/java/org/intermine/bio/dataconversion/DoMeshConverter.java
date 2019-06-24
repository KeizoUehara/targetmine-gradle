package org.intermine.bio.dataconversion;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.xml.full.Item;


/**
 * 
 * @author chenyian
 */
public class DoMeshConverter extends BioFileConverter
{
    //
//    private static final String DATASET_TITLE = "Add DataSet.title here";
//    private static final String DATA_SOURCE_NAME = "Add DataSource.name here";

    /**
     * Constructor
     * @param writer the ItemWriter used to handle the resultant items
     * @param model the Model
     */
    public DoMeshConverter(ItemWriter writer, Model model) {
        super(writer, model);
    }

    /**
     * 
     *
     * {@inheritDoc}
     */
    public void process(Reader reader) throws Exception {
    	BufferedReader in = new BufferedReader(reader);
    	String line;
    	boolean isTerm = false;
    	String identifier = null;
    	boolean isObsolete = false;
    	Set<String> meshIdSet = new HashSet<String>();
    	Set<String> cuiSet = new HashSet<String>();
    	while ((line = in.readLine()) != null) {
    		if (line.equals("[Term]")) {
    			isTerm = true;
    		} else if (line.startsWith("id:")) {
				identifier = line.substring(4);
    		} else if (line.startsWith("xref: MESH:")) {
    			String meshIdentifier = line.substring(line.indexOf("MESH:") + 5);
    			if (!"".equals(meshIdentifier)) {
    				meshIdSet.add(meshIdentifier);
    			}
    		} else if (line.startsWith("xref: UMLS_CUI:")) {
		    String cui = line.substring("xref: UMLS_CUI:".length());
    			if (!"".equals(cui)) {
    				cuiSet.add(cui);
    			}
    		} else if ("is_obsolete: true".equals(line.trim())) {
    			isObsolete = true;
    		} else if ("".equals(line.trim())) {
    			if (isTerm && !isObsolete) {
    				Item doTerm = createItem("DOTerm");
    				doTerm.setAttribute("identifier", identifier);
    				doTerm.setReference("ontology", getOntology("DO"));
    				for (String meshIdentifier : meshIdSet) {
    					doTerm.addToCollection("crossReferences", getMeshTerm(meshIdentifier));
    				}
    				for (String cui : cuiSet) {
        				Item umlsTerm = getIntegratedTerm(cui);
        				Item doIntegratedTerm = createItem("DOIntegratedTerm");
        				doIntegratedTerm.setReference("umls", umlsTerm);
        				doIntegratedTerm.setReference("doterm", doTerm);
        				store(doIntegratedTerm);
    				}
    				store(doTerm);
    			}
    			isTerm = false;
    			isObsolete = false;
    			meshIdSet = new HashSet<String>();
    			cuiSet = new HashSet<String>();
    		}
    		
    	}

    }
    
    private Map<String, Item> integratedTermMap = new HashMap<String, Item>();
    private Item getIntegratedTerm(String cui) throws ObjectStoreException {
        Item item = integratedTermMap.get(cui);
        if (item == null) {
	    item = createItem("IntegratedTerm");
	    item.setAttribute("identifier", cui);
	    store(item);
	    integratedTermMap.put(cui, item);
        }
        return item;
    }
    private Map<String, String> meshTermMap = new HashMap<String, String>();
    private String getMeshTerm(String meshIdentifier) throws ObjectStoreException {
    	String ret = meshTermMap.get(meshIdentifier);
    	if (ret == null) {
    		Item item = createItem("MeshTerm");
    		item.setAttribute("identifier", meshIdentifier);
    		item.setReference("ontology", getOntology("MeSH"));
    		store(item);
    		ret = item.getIdentifier();
    		meshTermMap.put(meshIdentifier, ret);
    	}
    	return ret;
    }

    private Map<String, String> ontologyMap = new HashMap<String, String>();
    private String getOntology(String name) throws ObjectStoreException {
    	String ret = ontologyMap.get(name);
    	if (ret == null) {
    		Item item = createItem("Ontology");
    		item.setAttribute("name", name);
    		store(item);
    		ret = item.getIdentifier();
    		ontologyMap.put(name, ret);
    	}
    	return ret;
    }

}
