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
import java.io.Reader;
import java.util.Iterator;

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

	private Map<String, Item> umlsItemMap = new HashMap<String, Item>();

	private Item getTermItem(String identifier,String name) throws ObjectStoreException {
		Item item = umlsItemMap.get(identifier);
		if(item!=null) {
			return item;
		}
		item = createItem("IntegratedTerm");
		item.setAttribute("identifier", identifier);
		item.setAttribute("name", name);
		store(item);
		umlsItemMap.put(identifier, item);
		return item;
	}

	/**
	 * 
	 *
	 * {@inheritDoc}
	 */
	public void process(Reader reader) throws Exception {
		Iterator<String[]> iterator = FormattedTextParser.parseDelimitedReader(reader,'|');
		while(iterator.hasNext()) {
			String[] mrConsoRow  = iterator.next();
			String identifier = mrConsoRow [0];
			Item item = getTermItem(identifier, mrConsoRow [14]);
			String sourceName = mrConsoRow [11];
			if("MSH".equals(sourceName)) {
				String code = mrConsoRow [13];
				Item meshItem = createItem("MeshTerm");
				meshItem.setAttribute("identifier",code);
				store(meshItem);
				Item meshIntegratedItem = createItem("MeshIntegratedTerm");
				meshIntegratedItem.setReference("cui", item);
				meshIntegratedItem.setReference("mesh", meshItem);
				store(meshIntegratedItem);
			}else if("GO".equals(sourceName)) {
				String code = mrConsoRow [13];
				Item goItem = createItem("GoTerm");
				goItem.setAttribute("identifier",code);
				store(goItem);
				Item goIntegratedItem = createItem("GoIntegratedTerm");
				goIntegratedItem.setReference("cui", item);
				goIntegratedItem.setReference("go", goItem);
				store(goIntegratedItem);
			}
		}
	}
}
