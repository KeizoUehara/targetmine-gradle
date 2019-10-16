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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
	private static Map<String, String> propertyNames = new HashMap<String, String>();

	static {
		propertyNames.put("identifier", "drugPrimaryName");
		propertyNames.put("overview", "overview");
		propertyNames.put("icd9", "icd9");
		propertyNames.put("icd10", "icd10");
		propertyNames.put("preClinical", "preClinical");
		propertyNames.put("phaseI", "phaseI");
		propertyNames.put("phaseII", "phaseII");
		propertyNames.put("phaseIII", "phaseIII");
		propertyNames.put("mechanismsOfAction", "mechanismsOfAction");
		propertyNames.put("originator", "originator");
		propertyNames.put("therapeuticClasses", "therapeuticClasses");
		propertyNames.put("pharmacokinetics", "pharmacokinetics");
		propertyNames.put("patents", "patents");
		propertyNames.put("marketing", "marketing");
		propertyNames.put("recordUrl", "recordUrl");
	}

	public void createPharmaProject(JSONObject item) throws ObjectStoreException {
		Item project = createItem("PharmaProject");
		for (Entry<String, String> entry : propertyNames.entrySet()) {
			Object opt = item.opt(entry.getValue());
			if(opt!=null && opt.toString().length() > 0) {
				project.setAttribute(entry.getKey(), opt.toString());
			}
		}
		store(project);
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

    /**
     * 
     *
     * {@inheritDoc}
     */
    public void process(Reader reader) throws Exception {
        JSONObject jsonObject = new JSONObject(readAll(reader));
        JSONArray jsonArray = jsonObject.getJSONArray("items");
        for (int i = 0; i < jsonArray.length(); i++) {
        	JSONObject item = jsonArray.getJSONObject(i);
        	createPharmaProject(item);
		}
    }
}
