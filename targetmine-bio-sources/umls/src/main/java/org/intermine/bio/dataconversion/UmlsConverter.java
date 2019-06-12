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
import java.util.Iterator;

import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.Model;
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

    /**
     * 
     *
     * {@inheritDoc}
     */
    public void process(Reader reader) throws Exception {
    	Iterator<String[]> iterator = FormattedTextParser.parseDelimitedReader(reader,'|');
    	while(iterator.hasNext()) {
    		String[] mrConsoRow  = iterator.next();
    		Item item = createItem("IntegratedTerm");
    		item.setAttribute("identifier", mrConsoRow [0]);
    		item.setAttribute("name", mrConsoRow [14]);
    		store(item);
    	}
    }
}
