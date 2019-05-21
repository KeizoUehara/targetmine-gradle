package org.intermine.bio.dataconversion;

import java.io.*;

//import org.apache.log4j.Logger;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.xml.full.Item;
import org.json.JSONObject;
/**
 * @author Ishikawa.Motokazu
 */
public class WhoTrialConverter extends BioFileConverter {
	
	private static final Logger LOG = LogManager.getLogger( WhoTrialConverter.class );
	
	private static final String DATASET_TITLE = "GOBIOM";
	private static final String DATA_SOURCE_NAME = "GOBIOM";
	

	/**
	 * Construct a new GobiomConverter.
	 * 
	 * @param model
	 *            the Model used by the object store we will write to with the ItemWriter
	 * @param writer
	 *            an ItemWriter used to handle Items created
	 */
	public WhoTrialConverter(ItemWriter writer, Model model) {
		super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
	}

	/**
	 * {@inheritDoc}
	 */
	public void process(Reader reader) throws Exception {
		
		LOG.info("Start to process WHO Trials");
		try(BufferedReader br = new BufferedReader(reader)){
			String line = br.readLine();
			JSONObject object = new JSONObject(line);
			JSONObject main = object.getJSONObject("main");
			Item item = createItem( "WhoTrial" );
			item.setIdentifier(main.get("Main ID").toString());
			item.setAttribute("title",main.get("Public title").toString());
			item.setAttribute("healthcondition",main.get("disease").toString());
			store(item);
		}

	}

}
