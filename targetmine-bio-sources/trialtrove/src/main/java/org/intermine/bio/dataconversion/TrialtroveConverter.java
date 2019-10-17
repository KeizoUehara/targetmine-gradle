package org.intermine.bio.dataconversion;

import java.io.BufferedReader;

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

import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.xml.full.Item;


/**
 * 
 * @author
 */
public class TrialtroveConverter extends BioFileConverter
{
    //
    private static final String DATASET_TITLE = "TrialTrove";
    private static final String DATA_SOURCE_NAME = "TrialTrove";

    /**
     * Constructor
     * @param writer the ItemWriter used to handle the resultant items
     * @param model the Model
     */
    public TrialtroveConverter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
    }
	private static Map<String, String> propertyNames = new HashMap<String, String>();

	static {
		propertyNames.put("name", "TrialTroveID-${trialId}");
		propertyNames.put("title", "${trialTitle}");
		propertyNames.put("primaryOutcome", "${trialPrimaryEndPoint}");
		propertyNames.put("secondaryOutcome", "${trialOtherEndPoint}");
		propertyNames.put("phase", "${trialPhase}");
		propertyNames.put("criteria", "${trialInclusionCriteria} ${trialExclusionCriteria}");
		propertyNames.put("result", "${trialOutcomes} ${trialOutcomeDetails}");
		propertyNames.put("countries", "${trialCountries}");
		propertyNames.put("primaryDrugName", "${trialPrimaryDrugsTested.drugName}");
		propertyNames.put("otherDrugName", "${trialOtherDrugsTested.drugName}");
		propertyNames.put("studyDesign", "${trialStudyDesign}");
		propertyNames.put("treatmentPlan", "${trialTreatmentPlan}");
	}
    /**
     * 
     *
     * {@inheritDoc}
     */
    public void process(Reader reader) throws Exception {
    	try(CSVParser parser = new CSVParser(reader)){
    		for(Map<String,String> row:parser) {
    			Item item = createItem("TrialTrove");
    			for (Entry<String,String> entry : propertyNames.entrySet()) {
					String value = Utils.replaceString(entry.getValue(), row);
					if(value!=null && value.length()>0) {
						item.setAttribute(entry.getKey(), value);
					}
				}
    			store(item);
    		}
    	}
    }
}
