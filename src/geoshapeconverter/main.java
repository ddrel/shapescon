package geoshapeconverter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.gdal.ogr.ogr;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import org.apache.commons.io.FilenameUtils;
import org.bson.types.ObjectId;

public class main {		
	    
	private static class  RBISmongo{
		static MongoClient mongoclient;
		static DB db;
		public static void  init(String server,int port,String dbname) {
			try {
				mongoclient = new MongoClient(server,port);
				db = mongoclient.getDB(dbname);
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();				
			}	    			
		}
		
		public static DBCollection getCollection(String table){
			return db.getCollection(table); 
		} 	
	}
	
	
	private static RBISmongo rbi;
	
	static MongoClient mongoclient;
	static DB db;
	public static void main(String[] args) throws ExecuteException, IOException {
		//ogr2ogr.main(cmd);		
		//ogr2ogr.LoadGeometry(pszDS, pszSQL, pszLyr, pszWhere)
		//String[] strcmd = {"ogr2ogr -f 'GeoJSON' -t_srs EPSG:4326 C:/shapes/RoadBridges.geojson C:/shapes/RoadBridges.shp"};
		//ogr.RegisterAll();
		//args = ogr.GeneralCmdLineProcessor(strcmd, 0 );	
		try {
			mongoclient = new MongoClient("localhost",27017);
			db = mongoclient.getDB("rbis2");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();				
		}	
        
        String source = "C:/Users/IBM_ADMIN/Desktop/old obando/SEPTEMBER_2016/SEPTEMBER 2016 DELIVERY/";        
        //File[] files = new File(source).listFiles();
        //recursivefiles(files);

        //String ps = readFile("C:/shapes/output/Aklan Province/Roads.geojson", StandardCharsets.UTF_8);
        //processtogeojson(ps,"Roads");
        
        //String ps2 = readFile("C:/shapes/output/Aklan Province/RoadBridges.geojson", StandardCharsets.UTF_8);
        //processtogeojson(ps2,"RoadBridges");
                
        ArrayList<String> filelist = new ArrayList<String>();
        File[] geofiles = new File("C:/shapes/output/").listFiles();
        
        System.out.println("Start Roads>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        //recursivesavetomongo(geofiles,"Roads");
        recursivesavetomongo(geofiles,"Roads",filelist);
        System.out.println("End Roads>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        
        System.out.println("Start Attributes Roads>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        //recursivesaveRoadtomongo(geofiles,"Roads");
        System.out.println("End Attributes Roads>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        
        /**/        
		for(String path:filelist){							
				File ff =  new File(path);
				String fname = FilenameUtils.removeExtension(ff.getName());
				if(!fname.equalsIgnoreCase("roads")){						
					System.out.println(">>>>" + path);
					String ps3 = readFile(path, StandardCharsets.UTF_8);
					processtogeojson(ps3,fname);
					//Thread.sleep(2000);
				
				}
		}
		
        //rbi  = RBISmongo();
        //RBISmongo.init("localhost", 27107,"rbis2");

	}
	
private static void recursivesavetomongo(File files[],String type,ArrayList<String> filearr){
		
		for (File file : files) {
	        if (file.isDirectory()) {
	        	recursivesavetomongo(file.listFiles(),type,filearr);
	        } else {
	        	
				try {
		        	String ps= readFile(file.getPath(), StandardCharsets.UTF_8);					
		        	String name = FilenameUtils.removeExtension(file.getName());
		        	System.out.println(name);
		        	
		        	if(name.equalsIgnoreCase("Roads") && file.getPath().indexOf(".geojson")>-1){
		        		processtogeojson(ps,"Roads");
		        		filearr.add(file.getPath());
		        	}
		        	else if(type!="" && file.getPath().indexOf(".geojson")>-1){
		        		//System.out.println(type);
		        		//System.out.println(type!="" && !type.equalsIgnoreCase("roads") && file.getPath().indexOf(".geojson")>-1);		        		
		        		System.out.println(file.getPath()); 		        		
		        		System.out.println(name);
		        		//processtogeojson(ps,name);
		        		filearr.add(file.getPath());
		        	}		        	
		        	
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	            
	        	
	        }
	   }
		
	}
	

	private static boolean ogr2ogr(File file){
			
		String geojpath = "C:/shapes/output/" + file.getParentFile().getParentFile().getName() + "/"+ file.getName().replace(".shp", ".geojson");
		String shpPath = file.getPath().replace("\\", "/");
		System.out.println(geojpath);
				
		File dir = new File("C:/shapes/output/" + file.getParentFile().getParentFile().getName());
		
		if(!dir.exists()){
			dir.mkdir(); 
		}
		
		String[] cmd = {"ogr2ogr","-t_srs", "EPSG:4326", "-f", "\"GeoJSON\"",geojpath,shpPath};				
		String line = "C:/Program Files/QGIS 2.18/OSGeo4W.bat";
        CommandLine commandLine = CommandLine.parse(line);
        commandLine.addArguments(cmd);
        DefaultExecutor executor = new DefaultExecutor();
        try {
			executor.execute(commandLine);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}	
		return true;
	}
	
	private static boolean processroads(JsonObject feature,DBCollection table){
		
		BasicDBObject searchQuery = new BasicDBObject();
		JsonObject fields = feature.get("properties").getAsJsonObject();		
		Set<Entry<String, JsonElement>> entrySet = fields.entrySet();
				
		searchQuery.put("R_ID", fields.get("R_ID").getAsString());
		DBCursor cursor = table.find(searchQuery);
		if (cursor.length()>0){
			DBCollection roads = db.getCollection("Roads");

			BasicDBObject removequery = new BasicDBObject();
			removequery.append("R_ID", fields.get("R_ID").getAsString());
			roads.remove(removequery);
		}
		
		
		//BasicDBObject document = new BasicDBObject();
		String jsr = "{";
		for(Entry<String, JsonElement> entry : entrySet){
			String key = entry.getKey();
			jsr+="\"" + key + "\"" + ":" + entry.getValue() + "" + ",";
			
		};
		
		jsr+="RoadBridges:[],RoadCarriageway:[],RoadCauseways:[],RoadCulverts:[],RoadDitches:[],RoadGuardrails:[],";
		jsr+="RoadHazards:[],RoadJunctions:[],RoadLightings:[],RoadLocRefPoints:[],RoadMarkings:[],RoadMedian:[],RoadPlaceNames:[],";
		jsr+="RoadShoulders:[],RoadSideFriction:[],RoadSideSlopes:[],RoadSideWalks:[],RoadSigns:[],RoadSpillways:[],RoadStructures:[],";
		
		//insert the points				
		Object  d =  com.mongodb.util.JSON.parse(feature.get("geometry").getAsJsonObject().toString());
		//System.out.println(d);
		jsr+="" + "\"geometry\":" + d + "}";
	
		//System.out.println("Roads  " + fields.get("R_ID"));
		
		DBObject jsonObject = (DBObject) com.mongodb.util.JSON.parse(jsr);
		table.insert(jsonObject);

		return true;
	};
	
	private static void  processroadsattrib(String namefile,JsonObject feature,DBCollection roads){				
		JsonObject fields = feature.get("properties").getAsJsonObject();				
		String road_key = "R_ID";		
		Set<Entry<String, JsonElement>> entrySet = fields.entrySet();
		
		String _oid = new ObjectId().get().toString();		
		String jsr = "{$push:{\"" + namefile + "\":{";
		jsr+="\"_id\":{\"$oid\":\"" + _oid +"\"},";
		for(Entry<String, JsonElement> entry : entrySet){
			String key = entry.getKey();
			
			//catch for RoadLocRefPoints
			if(key.equalsIgnoreCase("RoadID")){
				key =  new String("R_ID");
				road_key = "RoadID";
			}
			jsr+="\"" + key + "\"" + ":" + entry.getValue() + "" + ",";			
		}
		
				
		//insert the points				
		Object  d =  com.mongodb.util.JSON.parse(feature.get("geometry").getAsJsonObject().toString());
		//System.out.println(d);
		jsr+="" + "\"geometry\":" + d + "}}}";
	
		DBObject jsonObject = (DBObject) com.mongodb.util.JSON.parse(jsr);		
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("R_ID", fields.get(road_key).getAsString());
		roads.update(searchQuery, jsonObject);
	};
	
	private static void processtogeojson(String str,String _table){
				
		JsonParser parser = new JsonParser();
		JsonElement jsonTree = parser.parse(str);	
			
		
		if(jsonTree.isJsonObject()){
			JsonObject json = jsonTree.getAsJsonObject();			
			JsonArray features = json.get("features").getAsJsonArray();
			System.out.println(_table + "  >>>>>>> length: " + features.size());
			DBCollection table =  db.getCollection("Roads");		
			for(int i = 0; i < features .size(); i++){										
				JsonObject feature = features .get(i).getAsJsonObject();								
	
				if(_table.equalsIgnoreCase("Roads")){
					processroads(feature,table);
				}else{
					processroadsattrib(_table,feature,table);
				}
											
				}// end for features				
		}
	}
	
	
		
	private static void recursivefiles(File files[]){
	
		for (File file : files) {
	        if (file.isDirectory()) {
	        	recursivefiles(file.listFiles());
	        } else {
	        	if(file.getPath().indexOf(".shp")>-1 && file.getPath().indexOf(".xml")==-1){
	        		processfiles(file);	
	        	}
	        	
	        }
	   }
		
	}
	
	
	private static void processfiles(File file){
		try {
			
			if(ogr2ogr(file)){
				String ps = readFile(file.getPath(), StandardCharsets.UTF_8);
			}
			//			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private static String readFile(String path, Charset encoding) throws IOException 
	{
	  byte[] encoded = Files.readAllBytes(Paths.get(path));
	  return new String(encoded, encoding);
	}	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static void runcommand() throws IOException{
		Runtime rt = Runtime.getRuntime();
		String[] commands =  {"ogr2ogr -f 'GeoJSON' -t_srs EPSG:4326 C:/shapes/RoadBridges.geojson C:/shapes/RoadBridges.shp"};//{"system.exe","-get t"};
		String[] cmd = {"ogr2ogr","-t_srs", "EPSG:4326", "-f", "\"GeoJSON\"","C:/shapes/RoadBridges.geojson","C:/shapes/RoadBridges.shp"};
		Process proc = rt.exec(cmd);

		BufferedReader stdInput = new BufferedReader(new 
		     InputStreamReader(proc.getInputStream()));

		BufferedReader stdError = new BufferedReader(new 
		     InputStreamReader(proc.getErrorStream()));

		// read the output from the command
		
		String s = null;
		while ((s = stdInput.readLine()) != null) {
		    System.out.println(s);
		}

		// read any errors from the attempted command
		System.out.println("Here is the standard error of the command (if any):\n");
		while ((s = stdError.readLine()) != null) {
		    System.out.println(s);
		}
	}

}
