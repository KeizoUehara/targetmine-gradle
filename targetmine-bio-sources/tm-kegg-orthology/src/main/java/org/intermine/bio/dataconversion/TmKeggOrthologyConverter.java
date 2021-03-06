package org.intermine.bio.dataconversion;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.util.FormattedTextParser;
import org.intermine.xml.full.Item;

/**
 * The parser processes the file (ko_gene.list) downloaded from KEGG FTP. 
 * 
 * @author chenyian
 */
public class TmKeggOrthologyConverter extends BioFileConverter {
	protected static final Logger LOG = Logger.getLogger(TmKeggOrthologyConverter.class);
	//
	private static final String DATASET_TITLE = "KEGG Orthology";
	private static final String DATA_SOURCE_NAME = "KEGG";

	private static final String DROSOPHILA_MELANOGASTER_CODE = "dme";
	
	private Set<String> interestedOrganism = new HashSet<String>();

	private Map<String, String> geneMap = new HashMap<String, String>();

	public void setKeggOrganismCodes(String organismCodes) {
		this.interestedOrganism = new HashSet<String>(Arrays.asList(StringUtils.split(organismCodes, " ")));
		LOG.info("Setting list of organisms to " + this.interestedOrganism);
	}

	/**
	 * Constructor
	 * 
	 * @param writer
	 *            the ItemWriter used to handle the resultant items
	 * @param model
	 *            the Model
	 */
	public TmKeggOrthologyConverter(ItemWriter writer, Model model) {
		super(writer, model, DATA_SOURCE_NAME, DATASET_TITLE);
	}

	/**
	 * 
	 * 
	 * {@inheritDoc}
	 */
	public void process(Reader reader) throws Exception {
		if (interestedOrganism.isEmpty()) {
			throw new RuntimeException("No organism assigned.");
		}

		if (orgCodeTaxonIdMap.isEmpty()) {
			processTaxonRankFile();
		}
		
		if (interestedOrganism.contains(DROSOPHILA_MELANOGASTER_CODE) && flyIdMap.isEmpty()) {
			processFlyIdMapFile();
		}

		Iterator<String[]> iterator = FormattedTextParser.parseTabDelimitedReader(reader);
		Map<String, Set<String>> koMap = new HashMap<String, Set<String>>();
		while (iterator.hasNext()) {
			String[] cols = iterator.next();
			String organism = cols[1].substring(0, cols[1].indexOf(":"));
			if (interestedOrganism.contains(organism)) {
				if (koMap.get(cols[0]) == null) {
					koMap.put(cols[0], new HashSet<String>());
				}
				koMap.get(cols[0]).add(cols[1]);
			}
		}
		System.out.println("There are " + koMap.keySet().size() + " KO entries.");

		for (String koId: koMap.keySet()) {	
			Set<String> genes = koMap.get(koId);
			Item item = createItem("Homology");
			item.setAttribute("identifier", koId);
			for (String gene : genes) {
				String orgCode = gene.substring(0, gene.indexOf(":"));
				String taxonId = orgCodeTaxonIdMap.get(orgCode);
				String geneId = gene.substring(gene.indexOf(":") + 1);
				if (orgCode.equals(DROSOPHILA_MELANOGASTER_CODE)) {
					geneId = flyIdMap.get(geneId);
					if (geneId == null) {
						LOG.info(String.format("Cannot resolve the entry: %s in %s, skip this one.", gene, koId));
						continue;
					}
				}
				item.addToCollection("genes", getGene(geneId, taxonId));
			}
			store(item);
		}

	}

	private String getGene(String geneId, String taxonId) throws ObjectStoreException {
		String ret = geneMap.get(geneId);
		if (ret == null) {
			Item item = createItem("Gene");
			item.setAttribute("primaryIdentifier", geneId);
			item.setReference("organism", getOrganism(taxonId));
			ret = item.getIdentifier();
			store(item);
			geneMap.put(geneId, ret);
		}
		return ret;
	}
	
	private File taxonRankFile;

	public void setTaxonRankFile(File taxonRankFile) {
		this.taxonRankFile = taxonRankFile;
	}
	private Map<String, String> orgCodeTaxonIdMap = new HashMap<String, String>();
	private void processTaxonRankFile() throws Exception {
		Iterator<String[]> iterator = FormattedTextParser.parseTabDelimitedReader(new FileReader(taxonRankFile));
		while (iterator.hasNext()) {
			String[] cols = iterator.next();
			orgCodeTaxonIdMap.put(cols[0], cols[1]);
		}
	}

	private File flyIdMapFile;

	public void setFlyIdMapFile(File flyIdMapFile) {
		this.flyIdMapFile = flyIdMapFile;
	}
	
	private Map<String, String> flyIdMap = new HashMap<String, String>();
	private void processFlyIdMapFile() throws Exception {
		Iterator<String[]> iterator = FormattedTextParser.parseTabDelimitedReader(new FileReader(flyIdMapFile));
		while (iterator.hasNext()) {
			String[] cols = iterator.next();
			flyIdMap.put(cols[3], cols[1]);
		}
	}
}
