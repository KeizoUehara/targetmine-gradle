package org.intermine.bio.dataconversion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.HashMap;

public class UMLSParser implements AutoCloseable{
	public static final String[] DATA_TYPES = new String[]{
			"B2.2.1.2",//Pathologic Function
			"B2.3",//Injuery or Poisoning
			"A2.2.2",//Sign or Symptom
			"A1.2.2"//Anatomical Abnormality
	};
	private String[] semanticTypes;
	private File styFile;
	private HashMap<String, String> semanticTypeMap;
	private HashMap<String, String> semanticTypeLabelMap = new HashMap<String, String>();
	
	public UMLSParser(File consoFile, File styFile,String[] semanticTypes) throws IOException {
		this.semanticTypes = semanticTypes;
		this.reader = new BufferedReader(new FileReader(consoFile));
		this.styFile = styFile;
		loadStyFile();
	}
	public UMLSParser(Reader reader, File styFile,String[] semanticTypes) throws IOException {
		this.semanticTypes = semanticTypes;
		this.reader = new BufferedReader(reader);
		this.styFile = styFile;
		loadStyFile();
	}

	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	private void loadStyFile() throws IOException {
		if(styFile==null || semanticTypes==null) {
			return ;
		}
		semanticTypeMap = new HashMap<>();
		Files.lines(styFile.toPath()).forEach(line ->{
			String[] split = line.split("\\|");
			String cui = split[0];
			String str = split[2];
			String label = split[3];
			for(String type :DATA_TYPES){
				if(str.startsWith(type)) {
					semanticTypeMap.put(cui,type);
					if(!semanticTypeLabelMap.containsKey(type)) {
						semanticTypeLabelMap.put(type, label);
					}
					break;
				}
			}
		});
	}
	public void close() throws IOException {
		if(reader!=null) {
			reader.close();
			reader=null;
		}
	}
	private BufferedReader reader;
	public UMLS getNext() throws IOException {
		if(reader==null) {
			return null;
		}
		String line = null;
		while((line=reader.readLine())!=null) {
			String[] split = line.split("\\|");
			String identifer = split[0];
			if(semanticTypeMap== null || semanticTypeMap.containsKey(identifer)) {
				String name = split[14];
				String semanticType = semanticTypeMap.get(identifer);
				String label = semanticTypeLabelMap.get(semanticType);
				return new UMLS(identifer,name,semanticType,label,split[11],split[13]);
			}

		}
		close();
		return null;
		
	}
}
