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
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.io.Reader;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.nio.file.Files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;


/**
 * 
 * @author
 */
public class UmlsConverter extends BioFileConverter
{
	private static final Logger LOG = LogManager.getLogger( UmlsConverter.class );


	//
	private static final String DATASET_TITLE = "UMLS";
	private static final String DATA_SOURCE_NAME = "UMLS";
	/**
	 * Constructor
	 * @param writer the ItemWriter used to handle the resultant items
	 * @param model the Model
	 */
	public UmlsConverter(ItemWriter writer, Model model) {
		super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
	}
	private File semanticFile;
	public void setSemanticFile(File semanticFile) {
		this.semanticFile = semanticFile;
	}
	private Map<String, String> semanticTypeMap = new HashMap<String, String>();
	private void loadSemanticType() throws IOException {
		LOG.debug("semanticFile = "+semanticFile);
		Files.lines(semanticFile.toPath()).forEach(line ->{
			String[] split = line.split("\\|");
			String cui = split[0];
			if(split[2].startsWith("B2.2.1.2.1")) {
				semanticTypeMap.put(cui, split[3]);
			}
		});;
		LOG.debug("semanticTypeMap.size = "+semanticTypeMap.size());
	}
	private Map<String, Item> umlsItemMap = new HashMap<String, Item>();

	private Item getTermItem(String identifier,String name) throws ObjectStoreException {
		if(umlsItemMap.containsKey(identifier)) {
			return umlsItemMap.get(identifier);
		}
		Item item = null;
		if(semanticTypeMap.containsKey(identifier)) {
			item = createItem("IntegratedTerm");
			item.setAttribute("identifier", identifier);
			item.setAttribute("semanticType", semanticTypeMap.get(identifier));
			item.setAttribute("name", name);
			store(item);
		}
		umlsItemMap.put(identifier, item);
		return item;
	}

	/**
	 * 
	 *
	 * {@inheritDoc}
	 */
	public void process(Reader reader) throws Exception {
		loadSemanticType();
		Iterator<String[]> iterator = FormattedTextParser.parseDelimitedReader(reader,'|');
		while(iterator.hasNext()) {
			String[] mrConsoRow  = iterator.next();
			String identifier = mrConsoRow [0];
			Item item = getTermItem(identifier, mrConsoRow [14]);
			if(item==null){
				continue;
			}
			String sourceName = mrConsoRow [11];
			if("MSH".equals(sourceName)) {
				String code = mrConsoRow [13];
				creteMeshIntegratedTerm(code,item,identifier);
			}else if("GO".equals(sourceName)) {
				String code = mrConsoRow [13];
				creteGOIntegratedTerm(code,item,identifier);
			}else if("OMIM".equals(sourceName)) {
				String code = mrConsoRow [13];
				creteOMIMIntegratedTerm(code,item,identifier);
			}
		}
	}
	private Set<String> omimIntegratedTermMap = new HashSet<String>();
	private Item creteOMIMIntegratedTerm(String omimId,Item integratedTerm,String cui) throws ObjectStoreException {
		String key = omimId+":"+cui;
		if(omimIntegratedTermMap.contains(key)) {
			return null;
		}
		Item item = createItem("OMIMIntegratedTerm");
		item.setReference("omim", getOMIMTerm(omimId));
		item.setReference("cui", integratedTerm);
		store(item);
		omimIntegratedTermMap.add(key);
		return item;
	}
	private Set<String> goIntegratedTermMap = new HashSet<String>();
	private Item creteGOIntegratedTerm(String goId,Item integratedTerm,String cui) throws ObjectStoreException {
		String key = goId+":"+cui;
		if(goIntegratedTermMap.contains(key)) {
			return null;
		}
		Item item = createItem("GOIntegratedTerm");
		item.setReference("go", getGOTerm(goId));
		item.setReference("cui", integratedTerm);
		store(item);
		goIntegratedTermMap.add(key);
		return item;
	}
	private Set<String> meshIntegratedTermMap = new HashSet<String>();
	private Item creteMeshIntegratedTerm(String meshId,Item integratedTerm,String cui) throws ObjectStoreException {
		String key = meshId+":"+cui;
		if(meshIntegratedTermMap.contains(key)) {
			return null;
		}
		Item item = createItem("MeshIntegratedTerm");
		item.setReference("mesh", getMeshTerm(meshId));
		item.setReference("cui", integratedTerm);
		store(item);
		meshIntegratedTermMap.add(key);
		return item;
	}
	private Map<String, Item> omimTermMap = new HashMap<String, Item>();
	private Item getOMIMTerm(String omimId) throws ObjectStoreException {
		Item item = omimTermMap.get(omimId);
		if (item == null) {
			item = createItem("DiseaseTerm");
			item.setAttribute("identifier", omimId);
			item.setReference("ontology",getOntology("OMIM"));
			store(item);
			omimTermMap.put(omimId, item);
		}
		return item;
	}
	private Map<String, Item> meshTermMap = new HashMap<String, Item>();
	private Item getMeshTerm(String meshIdentifier) throws ObjectStoreException {
		Item item = meshTermMap.get(meshIdentifier);
		if (item == null) {
			item = createItem("MeshTerm");
			item.setAttribute("identifier", meshIdentifier);
			item.setReference("ontology",getOntology("MeSH"));
			store(item);
			meshTermMap.put(meshIdentifier, item);
		}
		return item;
	}
	private Map<String, Item> goTermMap = new HashMap<String, Item>();
	private Item getGOTerm(String goIdentifier) throws ObjectStoreException {
		Item item = goTermMap.get(goIdentifier);
		if (item == null) {
			item = createItem("GOTerm");
			item.setAttribute("identifier", goIdentifier);
			item.setReference("ontology",getOntology("GO"));
			store(item);
			goTermMap.put(goIdentifier, item);
		}
		return item;
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
