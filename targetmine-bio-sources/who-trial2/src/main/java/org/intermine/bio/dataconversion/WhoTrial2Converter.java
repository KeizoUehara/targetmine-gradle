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

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nu.xom.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;

/**
 * @author
 */
public class WhoTrial2Converter extends BioFileConverter {
    private static final Logger LOG = LogManager.getLogger(WhoTrial2Converter.class);
    private File mrConsoFile;
    private File mrStyFile;

    //
    private static final String DATASET_TITLE = "who-trial2";
    private static final String DATA_SOURCE_NAME = "who-trial2";

    // key is CUI, value is reference to UmlsDisease item
    private Map<String, Item> umlsDiseaseMap = new HashMap<String, Item>();

    /**
     * Constructor
     *
     * @param writer the ItemWriter used to handle the resultant items
     * @param model  the Model
     */
    public WhoTrial2Converter(ItemWriter writer, Model model) {
        super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
    }

    private static Map<String, String> propertyNames = new HashMap<String, String>();
    private static Map<String, String> whoTrial2XmlPropertyNames = new HashMap<String, String>();
    private static int STRING_LIMIT = 10000;
    private static int PRIMARY_KEY_STRING_LIMIT = 1000;

    private static final String WHO_TRIAL2_URL = "https://apps.who.int/trialsearch/Trial2.aspx?TrialID=%s";

    static {
        whoTrial2XmlPropertyNames.put("name", "TrialID");
        whoTrial2XmlPropertyNames.put("title", "Public_title");
        whoTrial2XmlPropertyNames.put("scientificTitle", "Scientific_title");
        whoTrial2XmlPropertyNames.put("studyType", "Study_type");
        whoTrial2XmlPropertyNames.put("recruitmentStatus", "Recruitment_Status");
        whoTrial2XmlPropertyNames.put("register", "Source_Register");
        whoTrial2XmlPropertyNames.put("primarySponsor", "Primary_sponsor");
        whoTrial2XmlPropertyNames.put("phase", "Phase");
        whoTrial2XmlPropertyNames.put("firstEnrolmentDate", "Date_enrollement");
        whoTrial2XmlPropertyNames.put("registrationDate", "Date_registration");
        whoTrial2XmlPropertyNames.put("lastRefreshed", "Last_Refreshed_on");
        whoTrial2XmlPropertyNames.put("targetSampleSize", "Target_size");
        whoTrial2XmlPropertyNames.put("originalUrl", "web_address");
        whoTrial2XmlPropertyNames.put("interventions", "Intervention");
        whoTrial2XmlPropertyNames.put("countries", "Countries");
        whoTrial2XmlPropertyNames.put("primaryOutcome", "Primary_outcome");
        whoTrial2XmlPropertyNames.put("secondaryOutcome", "Secondary_outcome");
    }

    private void storeTrialElements(Elements trialElements) {
        if(null == trialElements) {
            LOG.warn("Trial elements is null. read next file.");
            return;
        }
        for (int i = 0; i < trialElements.size(); i++) {
            Item whoTrial = createItem("ClinicalTrial");
            Element trial = trialElements.get(i);

            whoTrial2XmlPropertyNames.forEach((key, name) -> {
                Element child = trial.getFirstChildElement(name);
                if(child == null) {
                    LOG.warn("*****[test]Nothing Element : " + name);
                    return;
                }
                String elementValue = child.getValue().trim();
                if(elementValue != null && !elementValue.isEmpty()) {
                    if(elementValue.length() > STRING_LIMIT) {
                        LOG.warn("too large string at " + trial.getFirstChildElement("TrialID").getValue() + ", " + name + "= " + elementValue);
                    }
                    LOG.warn("[test]key : " + key + ", elementValue : " + elementValue);
                    whoTrial.setAttribute(key, elementValue);

                    if(name.equals("TrialID")) {
                        String url = String.format(WHO_TRIAL2_URL,elementValue);
                        whoTrial.setAttribute("url", url);
                    }
                }
            });

            // add disease.
            Element conditionElement = trial.getFirstChildElement("Condition");
            String condition = "";
            if(conditionElement != null) {
                condition = conditionElement.getValue();
            }

            String[] diseaseNameSet = convertConditionToDiseaseNameSet(condition);
            for(String diseaseName : diseaseNameSet){
                LOG.warn("[test]condition = " + diseaseName);
                if (diseaseName != null && diseaseName.length() > 0) {
                    Item trialTo = createItem("TrialToUmlsDisease");
                    trialTo.setAttribute("diseaseName", diseaseName);
                    try {
                        Item umlsDisease = getUmlsDisease(diseaseName);
                        trialTo.setReference("trial", whoTrial);
                        if (null != umlsDisease) {
                            trialTo.setReference("umls", umlsDisease);
                        }
                        store(trialTo);
                    } catch (ObjectStoreException e) {
                        LOG.warn("Cannot store who trials", e);
                    }
                }
            }

            try {
                store(whoTrial);
            } catch (ObjectStoreException e) {
                LOG.warn("Cannot store who trials", e);
            }
        }
    }

    private String[] convertConditionToDiseaseNameSet(String condition) {
        String[] diseaseNames = condition.split("<[Bb][Rr]>");//condition.split("\n");

        ArrayList<String> diseaseNameSet = new ArrayList<>();

        for(String diseaseName : diseaseNames) {
            diseaseName = diseaseName.trim();
            if(diseaseName != null && !diseaseName.isEmpty()) {
                if(diseaseName.length() > PRIMARY_KEY_STRING_LIMIT) {
                    LOG.warn("diseaseName OVER LIMTT 1000, str = " + diseaseName);
                    diseaseName = diseaseName.substring(0, PRIMARY_KEY_STRING_LIMIT);
                }

                diseaseNameSet.add(diseaseName);
                LOG.warn("Add disease :  ", diseaseName);
            }
        }
        return diseaseNameSet.toArray(new String[diseaseNameSet.size()]);
    }
    private Item getUmlsDisease(String umlsDiseaseName) throws ObjectStoreException {
        String cui = getCUI(umlsDiseaseName);
        if (cui == null) {
            return null;
        }
        Item item = umlsDiseaseMap.get(cui);
        if (item == null) {

            item = createItem("UmlsDisease");
            item.setAttribute("identifier", cui);
            store(item);
            umlsDiseaseMap.put(cui, item);
        }
        return item;

    }
    private static Pattern meddraPattern = Pattern.compile("Term:\\s+(.*)\\s*");
    private String getCUI(String diseaseName) {
        String cui = resolver.getIdentifier(diseaseName);
        if (cui != null) {
            return cui;
        }
        if(diseaseName.startsWith("MedDRA")) {
        	String[] lines = diseaseName.split("\n");
        	for (String line : lines) {
				Matcher matcher = meddraPattern.matcher(line);
				if(matcher.matches()) {
					cui = resolver.getIdentifier(matcher.group(1));
					return cui;
				}
			}
        }
        String[] split = diseaseName.split(";");
		if(split.length>1) {
			for (String string : split) {
				cui = resolver.getIdentifier(string);
				if(cui!=null) {
					return cui;
				}
			}
        }
    	return null;
    }
    private UMLSResolver resolver;
    /**
     * {@inheritDoc}
     */
    public void process(Reader reader) throws Exception {
        // mrConsoMap input data only once.
        // because, if project.xml( src.data.dir.includes) include multi files , the process method will be called multiple times.
        if(resolver==null) {
        	resolver = new UMLSResolver(mrConsoFile, mrStyFile);
        }

        LOG.warn("whoTrial2 start!");
        Document doc;
        Element rootElement;
        try {
            Builder parser = new Builder();
            doc = parser.build(reader);
            rootElement = doc.getRootElement();
            if(null == rootElement) {
                LOG.warn("RootElements is null. read next file.");
                return;
            }
        } catch (ParsingException e) {
            LOG.warn("Cannot XML parsing this file. read next file.");
            return;
        }

        Elements trialElements = rootElement.getChildElements("Trial");
        storeTrialElements(trialElements);
    }

    public void setMrConsoFile(File mrConsoFile) {
        this.mrConsoFile = mrConsoFile;
    }

    public void setMrStyFile( File mrStyFile ) {
        this.mrStyFile = mrStyFile;
    }

}