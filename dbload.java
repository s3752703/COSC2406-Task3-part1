import java.io.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

class Page{
	 ArrayList<ArrayList<Record>> page = new ArrayList<ArrayList<Record>>();
}

class Record{
	String ID;
	String Date_Time;
	String Year;
	String Month;
	String Mdate;
	String Day;
	String Time;
	String Sensor_ID;
	String Sensor_Name;
	String Hourly_Counts;

	public Record(String id,String dt,String yr,String mnth,String mdate,String day,String time,String ss_id,String ss_name,String hr_cnts){
		this.ID = id;
		this.Date_Time = dt;
		this.Year = yr;
		this.Month = mnth;
		this.Mdate = mdate;
		this.Day = day;
		this.Time = time;
		this.Sensor_ID = ss_id;
		this.Sensor_Name = ss_name;
		this.Hourly_Counts = hr_cnts;

	}
}


public class dbload {
	
	public static DataOutputStream outPutStream(int pageSize) throws FileNotFoundException,IOException {
	      DataOutputStream os = new DataOutputStream(new FileOutputStream("heap." + pageSize));
	      return os;
	}
	
	public static void HeapWriter(DataOutputStream os, File file, int pSize) {
		
        //CSVReader reader = new CSVReader(new FileReader(file),',');

        try(BufferedReader reader = new BufferedReader(new FileReader(file))){
			Page page = new Page();
			Record rc = null;
			
			int pgSize = pSize;
			int sizeCount = 0;
			int recCount = 0;
			int pgNo = 0;
			
			String lines;
			reader.readLine();
			while(( lines = reader.readLine())!=null){
				String[] values = lines.split(",");
				int sizeID = writing(values[0],os,3);
				int sizeDate_Time = writing(values[1],os,200);
				int sizeYear = writing(values[2],os,3);
				int sizeMonth = writing(values[3],os,20);
				int sizeMdate = writing(values[4],os,3);
				int sizeDay = writing(values[5],os,20);
				int sizeTime = writing(values[6],os,3);
				int sizeSensor_ID = writing(values[7],os,3);
				int sizeSensor_Name = writing(values[8],os,20);
				int sizeHourly_Counts = writing(values[9],os,3);
				rc = new Record(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9]);
				int recSize = sizeID + sizeDate_Time + sizeYear + sizeMonth + sizeDate_Time+ sizeDay + sizeTime + sizeSensor_ID + sizeSensor_Name + sizeHourly_Counts + sizeMdate;
	
				if((recSize + sizeCount)<pgSize){
					sizeCount = sizeCount + recSize;
					recCount+=1;
					page.page.add(new ArrayList<Record>());
					page.page.get(pgNo).add(rc);
				}else{
					pgNo++;
					page.page.add(new ArrayList<Record>());
					page.page.get(pgNo).add(rc);
					sizeCount = recSize;
				}
			
			}
			System.out.printf("Total number of page used:%d",page.page.size());
				int cnt = 0;
				for(int i = 0;i<page.page.size();i++){
					for(int j = 0;j<page.page.get(i).size();j++)
					cnt++;
				}
			System.out.printf("Number of record loaded:%d",cnt);
		}
		catch(IOException e){
			e.getStackTrace();
		}
        
        
	}
	
	public static int writing(String line, DataOutputStream os,int b) throws UnsupportedEncodingException,IOException{
		int Size = 0;
		if(getSize(line)<=b){
				byte[] src = line.getBytes(StandardCharsets.UTF_8);
				byte[] temp = Arrays.copyOf(src,b);
				os.write(temp);
				Size= temp.length;
				
			}
			else{
				byte[] src = line.getBytes(StandardCharsets.UTF_8);
				os.write(src);
				Size = src.length;
			}
		return Size;
	

	}

	public static int getSize(String string) throws UnsupportedEncodingException{
		final byte[] y = string.getBytes("UTF-8");
		int z = y.length;
		return z ;
	}

	public static void main(String[] args){
		long started = System.nanoTime();
		try{
			int temp = args.length - 2;
			int pgSize = Integer.parseInt(args[temp]);
			File f = new File(args[2]);
			DataOutputStream out = outPutStream(pgSize);
			HeapWriter(out,f,pgSize);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		long finished = System.nanoTime();
		long time = finished - started;
		System.out.printf("Heap file created in %d",time);
	}

	
	
}
