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

import org.apache.commons.lang.StringUtils;
import org.intermine.dataconversion.ItemWriter;
import org.intermine.metadata.ConstraintOp;
import org.intermine.metadata.Model;
import org.intermine.model.InterMineObject;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreFactory;
import org.intermine.objectstore.query.*;
import org.intermine.sql.Database;
import org.intermine.xml.full.Item;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * Integrate dbsnp in advance.
 * Read hgmd database in mysql.
 * mysql db setting wrote '~/.intermine/targetmine.properties'.
 * @author
 */
public class HgmdConverter extends BioDBConverter
{
    private static final Logger LOG = LogManager.getLogger(HgmdConverter.class);
    // 
    private static final String DATASET_TITLE = "hgmd";
    private static final String DATA_SOURCE_NAME = "hgmd";

    private Map<String, String> geneMap = new HashMap<String, String>();
    private Map<String, String> snpMap = new HashMap<String, String>();
    private Map<String, String> publicationMap = new HashMap<String, String>();

    private String osAlias = null;

    private Set<String> snpIdSet = new HashSet<String>();

    private Map<String, String> snpFunctionNameMap = new HashMap<String, String>();
    private Set<String> snpFunctionNameSet = new HashSet<String>();

    private static Map<String, String> mutypeToSnpFunctionNames = new HashMap<String, String>();
    static {
        Map<String, String> p = new HashMap<>();
        // key:hgmd_pro.allmut.mutype , value:snpfunction.name
        mutypeToSnpFunctionNames.put("frameshift", "frameshift");
        mutypeToSnpFunctionNames.put("missense", "missense");
        mutypeToSnpFunctionNames.put("nonsense", "STOP-GAIN");
        mutypeToSnpFunctionNames.put("nonstop", "STOP-LOSS");
        mutypeToSnpFunctionNames.put("synonymous", "cds-synon");
    }

    /**
     * Construct a new HgmdConverter.
     * @param database the database to read from
     * @param model the Model used by the object store we will write to with the ItemWriter
     * @param writer an ItemWriter used to handle Items created
     */
    public HgmdConverter(Database database, Model model, ItemWriter writer) {
        super(database, model, writer, DATA_SOURCE_NAME, DATASET_TITLE);
    }


    /**
     * {@inheritDoc}
     */
    public void process() throws Exception {
        getSnpIds();
        getSnpFunctionNames();

        // a database has been initialised from properties starting with db.hgmd

        Connection connection = getDatabase().getConnection();

        Statement stmt = connection.createStatement();

        // TODO: SQL 要確認
        String queryAllmut = "select * from hgmd_pro.allmut " +
                "LEFT JOIN hgmd_pro.mutnomen ON " +
                "hgmd_pro.allmut.acc_num = hgmd_pro.mutnomen.acc_num " +
                "LEFT JOIN hgmd_phenbase.hgmd_mutation ON " +
                "hgmd_pro.allmut.acc_num = hgmd_phenbase.hgmd_mutation.acc_num " +
                "LEFT JOIN hgmd_phenbase.phenotype_concept ON " +
                "hgmd_phenbase.hgmd_mutation.phen_id = hgmd_phenbase.phenotype_concept.phen_id " +
                // TODO : debug
                "limit 10 " +
                        ";";
        ResultSet resAllmut = stmt.executeQuery(queryAllmut);
        while (resAllmut.next()) {
            // hgmd data input. return hgmd id.
            String hgmdId = createHgmd(resAllmut);
            // publication data input & reference hgmd.
            getPublication(resAllmut, hgmdId);
            // snp data input & reference hgmd. return snpId.
            String snpId = getSnp(resAllmut, hgmdId);

            boolean isSnpContainsHgmdDbSnp = false;
            // if hgmd cntains dbsnp, only set reference snpid. else set snp and other data.
            if(!isSnpContainsHgmdDbSnp) {
                // SNPFunction data input. return SNPFunctionId.
                String snpFunctionId = getOrCreateSnpFunction(resAllmut);
                LOG.info("snpFunctionId : " + snpFunctionId );

                // get GeneId.
                String geneId = getGene(resAllmut);

                // VariationAnnotaition data input & reference geneId, SNPFunctionId, SNPId. return VariationAnnotation id.
                String variationAnnotationRef = getVariationAnnotation(geneId, snpFunctionId, snpId);

                // SNPReference data input & get SNPReference identifier.
                getSNPReference(resAllmut, snpFunctionId, variationAnnotationRef);
            }

        }

        stmt.close();
        connection.close();
    }

    private String createHgmd(ResultSet response) throws Exception {
        String identifier = response.getString("acc_num");
        String description = response.getString("descr");
        String variantClass = response.getString("tag");

        Item item = createItem("Hgmd");
        item.setAttribute("identifier", identifier);
        item.setAttribute("description", description);
        item.setAttribute("variantClass", variantClass);
        item.addToCollection("umlses", getUmlses(response));
        store(item);
        String hgmdId = item.getIdentifier();
        return hgmdId;
    }

    private String getPublication(ResultSet response, String hgmdId) throws Exception {
        String pubMedId = response.getString("pmid");
        LOG.warn("getPublication : " + pubMedId );
        String ret = publicationMap.get(pubMedId);

        if (ret == null) {
            // Publication set only pubMedId.
            Item item = createItem("Publication");
            item.setAttribute("pubMedId", pubMedId);
            item.setReference("hgmd", hgmdId);
            store(item);
            ret = item.getIdentifier();
            LOG.warn(" getPublication : pubmedid = "+ pubMedId +"publication identifer " + ret);
            geneMap.put(pubMedId, ret);
        }
        return ret;
    }

    private String getSnp(ResultSet response, String hgmdId) throws Exception {
        // hgmd にdbsnpがあればそのまま使用、なければacc_numで代用
        String identifier = response.getString("dbsnp");
        LOG.info("getSnp : dbsnp identifier " + identifier);
        if(StringUtils.isEmpty(identifier)) {
            identifier = response.getString("acc_num");
            LOG.info("getSnp : accnum identifier " + identifier);
        }

        String ret = snpMap.get(identifier);

        if(ret == null) {
            Item item = createItem("SNP");

            // hgmd にdbsnpのIDが入っていればSNP にリンクを張る。
            if(snpIdSet.contains(identifier)) {
                // itemへのstore後のidを取得したいため、identifierのみ設定
                item.setAttribute("identifier", identifier);
            } else {
                // 無い場合はhgmdのデータを利用してSNPの情報を埋める
                String coodStart = response.getString("startCoord");
                String chromosome = response.getString("chromosome");
                // TODO: データの作り方 要確認 : allmut.chromosomeとallmut.coordSTART
                String location = chromosome + ":" + coodStart;
                // TODO: データの作り方 要確認 : allmut.hgvsまたはallmut.deletionまたはallmut.insertion
                String refSnpAllele = "";
                if(!StringUtils.isEmpty(response.getString("hgvs"))) {
                    refSnpAllele = response.getString("hgvs");
                    LOG.info("getSnp : hgvs refSnpAllele " + refSnpAllele);
                }else if(!StringUtils.isEmpty(response.getString("deletion"))) {
                    refSnpAllele = response.getString("deletion");
                    LOG.info("getSnp : deletion refSnpAllele " + refSnpAllele);
                }else if(!StringUtils.isEmpty(response.getString("insertion"))) {
                    refSnpAllele = response.getString("insertion");
                    LOG.info("getSnp : insertion refSnpAllele " + refSnpAllele);
                }
                LOG.info("getSnp : coodStart " + coodStart);
                LOG.info("getSnp : chromosome " + chromosome);
                // TODO: データの作り方　要確認 :  ?
                String orientation = "";

                item.setAttribute("identifier", identifier);
                if (!StringUtils.isEmpty(location)) {
                    item.setAttribute("location", location);
                }
                if (!StringUtils.isEmpty(chromosome)) {
                    item.setAttribute("chromosome", chromosome);
                }
                if (!StringUtils.isEmpty(refSnpAllele)) {
                    item.setAttribute("refSnpAllele", refSnpAllele);
                }
                if (!StringUtils.isEmpty(orientation)) {
                    item.setAttribute("orientation", orientation);
                }
            }
            item.setReference("hgmd", hgmdId);
            store(item);
            ret = item.getIdentifier();
            snpMap.put(identifier, ret);
        }

        LOG.warn("getSnp : ret " + ret);
        return ret;
    }

    private String getUmlses(ResultSet response) throws Exception {
        // hgmd acc_num
        String acc_num = "";

        String cui = response.getString("cui");
        LOG.warn("getUmlses : cui " + cui);

        // Publication set only pubMedId.
        Item item = createItem("UMLSDisease");
        item.setAttribute("identifier", cui);
        store(item);
        LOG.warn(" getUmlses : identifier = "+ cui);
        return item.getIdentifier();
    }

    /**
     * DB read SNP column "identifier".
     * @throws Exception
     */
    private void getSnpIds() throws Exception {
        LOG.info("Start loading snp identifiers");
        ObjectStore os = ObjectStoreFactory.getObjectStore(osAlias);

        Query q = new Query();
        QueryClass qcSnp = new QueryClass(os.getModel().
                getClassDescriptorByName("SNP").getType());
        QueryField qfSnpId = new QueryField(qcSnp, "identifier");
        q.addFrom(qcSnp);
        q.addToSelect(qfSnpId);

        Results results = os.execute(q);
        Iterator<Object> iterator = results.iterator();

        LOG.info("SNP iterator before" );
        while (iterator.hasNext()) {
            ResultsRow<String> rr = (ResultsRow<String>) iterator.next();
            snpIdSet.add(rr.get(0));
            LOG.info(" loaded snp { id : " + rr.get(0) + "}" );
        }
        LOG.info("loaded "+ snpIdSet.size()+" SNP (size)" );
    }

    /**
     * DB read SNPFunction column "name".
     * @throws Exception
     */
    private void getSnpFunctionNames() throws Exception {
        LOG.info("Start loading snpfunction name");
        ObjectStore os = ObjectStoreFactory.getObjectStore(osAlias);

        Query q = new Query();
        QueryClass qcSnpFunction = new QueryClass(os.getModel().
                getClassDescriptorByName("SNPFunction").getType());

        q.addFrom(qcSnpFunction);
        q.addToSelect(qcSnpFunction);

        Results results = os.execute(q);
        Iterator<Object> iterator = results.iterator();

        LOG.info("iterator before" );
        while (iterator.hasNext()) {
            LOG.info("iterator start" );
            ResultsRow<InterMineObject> rr = (ResultsRow<InterMineObject>) iterator.next();
            InterMineObject p = rr.get(0);

            LOG.info("InterMineObject { p :"+ p + "}" );
            String name = (String) p.getFieldValue("name");
            LOG.info(" loaded snpFunction { name : " + name + "}" );

            if (name != null) {
                snpFunctionNameSet.add(name);
            }
        }
        LOG.info("loaded "+ snpFunctionNameSet.size()+" snpFunction (size)" );
    }

    private String getOrCreateSnpFunction(ResultSet response) throws Exception {
        String mutype = response.getString("mutype");

        if(StringUtils.isEmpty(mutype)) {
            LOG.info("SNPFunction is Emptiy.");
            return "";
        }

        // mapping mutype -> snpfunction name.
        String snpFunctionName = mutypeToSnpFunctionNames.get(mutype);
        if(snpFunctionName == null) {
            // not mapping. use mutype.
            snpFunctionName = mutype;
        }

        String ret = snpFunctionNameMap.get(snpFunctionName);
        if(ret == null) {
            // create item.
            Item item = createItem("SNPFunction");
            item.setAttribute("name", snpFunctionName);
            store(item);
            ret = item.getIdentifier();
            LOG.info(" create SNPFunction! name = "+ snpFunctionName + "item identifier = " + ret);
            snpFunctionNameMap.put(snpFunctionName, item.getIdentifier());
        }

        LOG.info(" snpfunction() end. getIdentifier = " + ret);
        return ret;
    }

    private String getGene(ResultSet response) throws Exception {
        // get refCore
        String refCore = response.getString("refCORE");

        // refCore contains synonym.intermine_value, get synonym.subjectid.
        Map<String, String> synonymMap = getSynonymItem(refCore);
        String geneId = synonymMap.get(refCore);

        return geneId;
    }

    private Map<String, String> getSynonymItem(String refCore) throws Exception {
        Map<String, String> synonymSubjectIdMap = new HashMap();

        LOG.info("Start loading synonym. refCore : " + refCore);
        ObjectStore os = ObjectStoreFactory.getObjectStore(osAlias);

        Query q = new Query();
        QueryClass qcSynonym = new QueryClass(os.getModel().
                getClassDescriptorByName("Synonym").getType());

        q.addFrom(qcSynonym);
        QueryField qcSynonymInterMineValue = new QueryField(qcSynonym, "value");
        SimpleConstraint sc = new SimpleConstraint(qcSynonymInterMineValue, ConstraintOp.EQUALS, new QueryValue(refCore));
        q.setConstraint(sc);

        Results results = os.execute(q);
        Iterator<Object> iterator = results.iterator();

        LOG.info("iterator before" );
        while (iterator.hasNext()) {
            ResultsRow<InterMineObject> rr = (ResultsRow<InterMineObject>) iterator.next();
            InterMineObject p = rr.get(0);

            LOG.info("InterMineObject { p :"+ p + "}" );
            String intermineValue = (String) p.getFieldValue("value");
            InterMineObject geneItem = (InterMineObject)p.getFieldValue("subject");
            Integer subjectId = (Integer) geneItem.getFieldValue("id");
            LOG.info(" loaded snpFunction { intermineValue : " + intermineValue + ",subjectId : " + subjectId + "}" );
            if (subjectId != null) {
                synonymSubjectIdMap.put(refCore, subjectId.toString());
            }
        }
        LOG.info("loaded "+ synonymSubjectIdMap.size()+" getSynonymItem (size)" );
        return synonymSubjectIdMap;
    }

//    private Set<String> getGeneId(String geneId) throws Exception {
//        Set<String> geneIdSet = new HashSet<String>();
//
//        LOG.info("Start loading gene");
//        ObjectStore os = ObjectStoreFactory.getObjectStore(osAlias);
//
//        Query q = new Query();
//        QueryClass qcGene = new QueryClass(os.getModel().
//                getClassDescriptorByName("Gene").getType());
//
//        q.addFrom(qcGene);
//        QueryField qcGeneId = new QueryField(qcGene, "id");
//        q.addToSelect(qcGeneId);
//
//        SimpleConstraint sc = new SimpleConstraint(qcGeneId, ConstraintOp.EQUALS, new QueryValue(geneId));
//        q.setConstraint(sc);
//
//        Results results = os.execute(q);
//        Iterator<Object> iterator = results.iterator();
//
//        LOG.info("iterator before" );
//        while (iterator.hasNext()) {
//            ResultsRow<InterMineObject> rr = (ResultsRow<InterMineObject>) iterator.next();
//            InterMineObject p = rr.get(0);
//
//            LOG.info("InterMineObject { p :"+ p + "}" );
//            Integer id = (Integer) p.getFieldValue("id");
//            LOG.info(" loaded snpFunction { intermineValue : " + id + "}" );
//
//            if (id != null) {
//                geneIdSet.add(id.toString());
//            }
//        }
//        LOG.info("loaded "+ geneIdSet.size()+" getGeneId (size)" );
//        return  geneIdSet;
//
//    }

    private String getGene(String ncbiGeneId) throws ObjectStoreException {
        String ret = geneMap.get(ncbiGeneId);

        if (ret == null) {
            Item item = createItem("Gene");
            item.setAttribute("primaryIdentifier", ncbiGeneId);
            item.setAttribute("ncbiGeneId", ncbiGeneId);
            store(item);
            ret = item.getIdentifier();
            geneMap.put(ncbiGeneId, ret);
        }
        return ret;
    }

    private String getVariationAnnotation(String geneId, String functionId, String snpId) throws ObjectStoreException {

        String identifier = snpId + "-" + geneId;
        LOG.info("getVariationAnnotation : identifier = " + identifier);
        Item item = createItem("VariationAnnotation");
        item.setAttribute("identifier", identifier);
        item.setReference("gene", geneId);
        item.setReference("function", functionId);
        item.setReference("snp", snpId);
        store(item);
        LOG.info("getVariationAnnotation : OK. identifier = " + item.getIdentifier());
        return item.getIdentifier();
    }

    private String getSNPReference(ResultSet response, String snpFunctionRef, String variationAnnotationRef) throws Exception {
        String mrnaAcc = response.getString("refCORE") + "." + response.getString("refVER"); // TODO: 文字列結合の方法?
        String mrnaPos = response.getString("cSTART");

        String orientation = ""; // TODO: データの作り方?
        String allele = response.getString("wildBASE") + " -> " + response.getString("mutBASE"); // TODO: 文字列結合の方法?
        String codon = ""; // TODO: データの作り方?
        String proteinAcc = response.getString("protCORE") + "." + response.getString("protVER"); // TODO: 文字列結合の方法?
        int aaPos = response.getInt("codon");
        String residue = response.getString("wildAMINO") + " -> " +  response.getString("mutAMINO"); // TODO: 文字列結合の方法?
        String funcRef = snpFunctionRef;
        String vaItemRef = variationAnnotationRef;

        return createSNPReference(mrnaAcc, mrnaPos, orientation, allele, codon, proteinAcc, aaPos, residue, funcRef, vaItemRef);
    }

    private String createSNPReference(String mrnaAcc, String mrnaPos, String orientation,
                                      String allele, String codon, String proteinAcc, int aaPos, String residue,
                                      String funcRef, String vaItemRef) throws ObjectStoreException {
        Item item = createItem("SNPReference");
        item.setAttribute("mrnaAccession", mrnaAcc);
        if (!StringUtils.isEmpty(mrnaPos)) {
            item.setAttribute("mrnaPosition", mrnaPos);
        }
        if (!StringUtils.isEmpty(orientation)) {
            item.setAttribute("orientation", orientation);
        }
        if (!StringUtils.isEmpty(allele)) {
            item.setAttribute("mrnaAllele", allele);
        }
        if (!StringUtils.isEmpty(codon)) {
            item.setAttribute("mrnaCodon", codon);
        }
        if (!StringUtils.isEmpty(proteinAcc)) {
            item.setAttribute("proteinAccession", proteinAcc);
        }
        if (aaPos > 0) {
            item.setAttribute("proteinPosition", String.valueOf(aaPos));
        }
        if (!StringUtils.isEmpty(residue)) {
            item.setAttribute("residue", residue);
        }
        if (!StringUtils.isEmpty(funcRef)) {
            item.setReference("function", funcRef);
        }
        if (!StringUtils.isEmpty(vaItemRef)) {
            item.setReference("annotation", vaItemRef);
        }

        store(item);

        return item.getIdentifier();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDataSetTitle(String taxonId) {
        return DATASET_TITLE;
    }

    public void setOsAlias(String osAlias) {
        this.osAlias = osAlias;
    }
}
