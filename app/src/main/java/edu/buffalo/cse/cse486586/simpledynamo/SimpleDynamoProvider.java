package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import static android.content.Context.MODE_PRIVATE;

public class SimpleDynamoProvider extends ContentProvider {
	

	private static final String[] Ports = {"11108", "11112", "11116", "11120", "11124"};
	private static final String[] Device_ID = {"5554", "5556", "5558", "5560", "5562"};

	static String thisNodeId = null;
	static String thisPort = null;
	static String thisNodeIdGenHash = null;

	private static ArrayList<String> nodesList = new ArrayList<String>();
	private HashMap<String,String> SenderHashMap = new HashMap();
	private HashMap<String, String> MapQuery= new HashMap<String, String>();
	private HashMap<String,HashMap<String,String>> MapQueryAll = null;
	private Integer CountQueryAll = 1;
	private Integer RepliesExpected = 4;
	public static boolean isRestart = false;
	public Uri myUri = null;

	@Override
	public boolean onCreate() {
		Log.i("my","OnCreate(): start");
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
		uriBuilder.scheme("content");
		myUri = uriBuilder.build();

		thisNodeId = portStr;
		thisPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try {
			thisNodeIdGenHash = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			Log.e("my", "OnCreate():Gen hash exception:"+e.toString());
		}

		try {
			ServerSocket serverSocket = new ServerSocket(10000);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {

			Log.e("my", "OnCreate():server socket creation exception:"+e.toString());
			return  false;
		}

		for ( int i = 0 ; i<Device_ID.length ; i++) {

			try {
				String genHashVal = genHash(Device_ID[i]);
				SenderHashMap.put(genHashVal, Device_ID[i]);
				nodesList.add(genHashVal);

			} catch(NoSuchAlgorithmException e ) {
				Log.e("my", "OnCreate() : No Such Algorithm exception caught ");
				e.printStackTrace();
			}
		}

		Collections.sort(nodesList, new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				return s1.compareToIgnoreCase(s2);
			}
		});

		SharedPreferences pref = getContext().getSharedPreferences("MyPref", MODE_PRIVATE);
		SharedPreferences.Editor editor = pref.edit();

		String prefvalue=pref.getString("restart", null);
		Log.i("my","OnCreate(): value of pref: "+prefvalue);
		if(prefvalue!="restart")
		{
			Log.i("my","OnCreate(): no recovery required");
			isRestart=false;
			editor.putString("restart", "string restart");
			Log.i("my","OnCreate(): Pref value added");
		}
		else
		{
			Log.i("my","OnCreate(): recovery required");
			isRestart=true;
			int indexthisNode = nodesList.indexOf(thisNodeIdGenHash);
			String mySuccessorNode = SenderHashMap.get(nodesList.get((indexthisNode+ 1) % 5));
			Message msgtosend= new Message();
			msgtosend.msgType=("13");
			msgtosend.ReceiverForMsg=(mySuccessorNode);
			msgtosend.senderNodeID=(thisNodeId);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
			Log.i("my","OnCreate(): recover original msgs sent");

			String Pred1 = null;
			String Pred2= null;
			if(indexthisNode==0)
			{
				Pred1= SenderHashMap.get(nodesList.get(4));
				Pred2= SenderHashMap.get(nodesList.get(3));
			}
			else if(indexthisNode==1)
			{
				Pred1= SenderHashMap.get(nodesList.get(0));
				Pred2= SenderHashMap.get(nodesList.get(4));
			}
			else
			{
				Pred1= SenderHashMap.get(nodesList.get((indexthisNode - 1) % 5));
				Pred2= SenderHashMap.get(nodesList.get((indexthisNode- 2) % 5));
			}

			Message msgtosend2 = new Message();
			msgtosend2.msgType=("12");
			msgtosend2.ReceiverForMsg=(Pred1);
			msgtosend2.successor1=(Pred2);
			msgtosend2.senderNodeID=(thisNodeId);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend2);
			Log.i("my","OnCreate(): recover get replicated msg sent");
		}

		Log.i("my","OnCreate(): end");
		return true;

	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if( selection.equals("@") ) {
			Log.i("my", "Delete(@): start");
			try {
				String fileslist[] = getContext().fileList();
				File dir = getContext().getFilesDir();
				for(int i = 0 ; i<fileslist.length ; i++) {
					File file = new File(dir, fileslist[i]);
					file.delete();
					Log.e("my", "Delete(@):Deleted file:"+i+" filename : "+fileslist[i]);
				}
			} catch (Exception e) {
				Log.e("my", "Delete(@):File delete failed");
				e.printStackTrace();
			}
			Log.v("my","delete(@):end");
			return 1;

		} else if (selection.equals("*")) {
			Log.i("my", "Delete(*): start");
			try {
				String fileslist[] = getContext().fileList();
				File dir = getContext().getFilesDir();
				for(int i = 0 ; i<fileslist.length ; i++) {
					File file = new File(dir, fileslist[i]);
					file.delete();
					Log.e("my", "Delete(*):Deleted file :"+i+" filename : "+fileslist[i]);
				}
			} catch (Exception e) {
				Log.e("my", "Delete(*):File delete failed");
				e.printStackTrace();
			}
			Message msgtosend = new Message();
			msgtosend.msgType=("6");
			msgtosend.typeofquery=("@");
			msgtosend.senderNodeID=(thisNodeId);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
			Log.v("my","delete(*):End");
			return 1;
		} else {
			Log.v("my","delete(direct):start");
			String filekey = selection;
			String filekeyhash = null;
			if(selectionArgs !=null) {
				try {
					File dir = getContext().getFilesDir();
					File file = new File(dir, filekey);
					file.delete();
					Log.v("my", "Delete(direct):file deleted"+filekey);
				} catch (Exception e) {
					Log.e("my", "delete(direct):File delete failed");
					e.printStackTrace();
				}
				return 1;
			}
			try {
				filekeyhash= genHash(filekey);
			} catch(NoSuchAlgorithmException e ) {
				Log.e("my", "Delete(direct): No Such Algorithm exception caught");
				e.printStackTrace();
			}

			int indexthisNode = nodesList.indexOf(thisNodeIdGenHash);
			int indexforKey = 0;
			if(((filekeyhash.compareTo(nodesList.get(4)))>0) ||(filekeyhash.compareTo(nodesList.get(0))<=0)) {
				indexforKey= 0;
			} else {
				for (int i =1 ; i<nodesList.size(); i++) {
					if( (filekeyhash.compareTo(nodesList.get(i-1))>0) && (filekeyhash.compareTo(nodesList.get(i))<=0) ) {
						indexforKey= i;
						break;
					}
				}
			}

			String originalNode = SenderHashMap.get(nodesList.get(indexforKey));
			String successor1 = SenderHashMap.get(nodesList.get((indexforKey+ 1) % 5));
			String successor2 = SenderHashMap.get(nodesList.get((indexforKey+2)%5));
			Log.e("my", "DELETE() : thisNodeId: "+thisNodeId+" originalNode: "+originalNode+ " successor1: "+successor1+
					"  successor2: "+successor2);

			if(indexthisNode == indexforKey) {
				try {
					File dir = getContext().getFilesDir();
					File file = new File(dir, filekey);
					file.delete();
					Log.i("my", "DELETE(direct) : file deleted:"+filekey);

				} catch (Exception e) {
					Log.e("my", "DELETE(direct):File delete failed");
					e.printStackTrace();
				}

				Message msgtosend = new Message();
				msgtosend.msgType=("4");
				msgtosend.typeofquery=(filekey);
				msgtosend.senderNodeID=(thisNodeId);
				msgtosend.successor1=(successor1);
				msgtosend.successor2=(successor2);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
				Log.i("my", "DELETE(direct) : delete replicated msg sent: " + msgtosend.toString());
			} else {
				Message msgtosend = new Message();
				msgtosend.msgType=("5");
				msgtosend.typeofquery=(filekey);
				msgtosend.senderNodeID=(thisNodeId);
				msgtosend.ReceiverForMsg=(originalNode);
				msgtosend.successor1=(successor1);
				msgtosend.successor2=(successor2);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
				Log.i("my", "DELETE(direct):DELETE FROM ALL msg sent" + msgtosend.toString());
			}
			Log.e("my", "DELETE(direct) :end)");
			return 1;
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		Log.i("my", "Insert():start");
		String key = (String)values.get("key");
		String value = (String)values.get("value");
		String keyhash = null;
		try {
			keyhash= genHash(key);
		} catch(NoSuchAlgorithmException e ) {
			Log.e("my", "Insert():Excpetion:"+e.toString());
		}
		int indexthisNode = nodesList.indexOf(thisNodeIdGenHash);
		int indexforKey = 0;
		if(((keyhash.compareTo(nodesList.get(4)))>0) ||(keyhash.compareTo(nodesList.get(0))<=0)) {
			indexforKey= 0;
		} else {
			for (int i =1 ; i<nodesList.size(); i++) {
				if( (keyhash.compareTo(nodesList.get(i-1))>0) && (keyhash.compareTo(nodesList.get(i))<=0) ) {
					indexforKey= i;
					break;
				}
			}
		}
		String originalNode = SenderHashMap.get(nodesList.get(indexforKey));
		String successor1= SenderHashMap.get(nodesList.get((indexforKey+1)%5));
		String successor2= SenderHashMap.get(nodesList.get((indexforKey+2)%5));
		if(indexthisNode== indexforKey) {
			//store this key value pair at this node
			FileOutputStream outputStream;
			try {
				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
				outputStream.write(value.getBytes());
				outputStream.flush();
				outputStream.close();
			} catch (Exception e) {
				Log.e("my", "Insert():Exception"+e.toString());
			}
			Message msgToSend= new Message();
			msgToSend.msgType=("2");
			msgToSend.ContentValues.put("key",key);
			msgToSend.ContentValues.put("value", value);
			msgToSend.senderNodeID=(thisNodeId);
			msgToSend.ReceiverForMsg=(thisNodeId);
			msgToSend.successor1=(successor1);
			msgToSend.successor2=(successor2);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
			Log.e("my", "Insert():INSERT replicated msgs sent to succ 1: " + successor1+ " succ2: " +successor2);
		} else {
			//send the request  to succ nodes for storage of this node
			Message msgToSend = new Message();
			msgToSend.msgType=("1");
			msgToSend.ContentValues.put("key",key);
			msgToSend.ContentValues.put("value", value);
			msgToSend.senderNodeID=(thisNodeId);
			msgToSend.ReceiverForMsg=(originalNode);
			msgToSend.successor1=(successor1);
			msgToSend.successor2=(successor2);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
			Log.e("my", "INSERT() : insert msg forwarded:" + msgToSend.toString());
		}
		Log.i("my", "Insert():end");
		return uri;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		if( selection.equals("@") ) {
			Log.i("my", "query(@):start");
			String[] temparray=new String[] { "key", "value"};
			MatrixCursor matrixcursor = new MatrixCursor(temparray);
			String fileList[] = getContext().fileList();
			for ( int cnt = 0 ; cnt< fileList.length; cnt++) {
				String filekey = fileList[cnt];
				StringBuffer fileData = new StringBuffer("");
				try {
					int noOfBytesRead=0;
					FileInputStream is = getContext().openFileInput(filekey);
					byte[] buffer = new byte[1024];
					while ((noOfBytesRead=is.read(buffer)) != -1)
					{
						fileData.append(new String(buffer, 0, noOfBytesRead));
					}
				}catch (IOException e) {
					Log.e("my", "Query(@): exception:"+e.toString());
				}
				matrixcursor.addRow(new String[] {filekey, fileData.toString() });
				Log.v("my", "Query(@):Filename : "+filekey+" & File data: "+fileData.toString());
			}
			Log.i("my","query(@):cursor count : "+matrixcursor.getCount());
			Log.i("my", "query(@):end");
			return matrixcursor;
		} else if (selection.equals("*")) {
			Log.i("my", "query(*):start");
			String[] temparray = new String[] { "key", "value"};
			MatrixCursor matrixcursor = new MatrixCursor(temparray);
			String fileNamesList[] = getContext().fileList();
			for ( int cnt= 0 ; cnt < fileNamesList.length; cnt++) {
				String filekey = fileNamesList[cnt];
				int noOfBytesRead=0;
				StringBuffer fileData = new StringBuffer("");
				try {
					FileInputStream is = getContext().openFileInput(filekey);
					byte[] buffer = new byte[1024];
					while ((noOfBytesRead=is.read(buffer)) != -1)
					{
						fileData.append(new String(buffer, 0, noOfBytesRead));
					}
				} catch (IOException e) {
					Log.e("my","Query(*):Exception:"+e.toString());
				}
				matrixcursor.addRow(new String[] {filekey, fileData.toString() });
				Log.i("my", "Query(*):Filekey : "+filekey+" & FileData: "+fileData.toString());
			}
			MapQueryAll = new HashMap<String,HashMap<String,String>>();

			for(int i=0;i<Device_ID.length;i++) {
				if(!thisNodeId.equals(Device_ID[i])) {
					MapQueryAll.put(Device_ID[i],null);
				}
			}
			Message msgToSend = new Message();
			msgToSend.msgType=("10");
			msgToSend.senderNodeID=(thisNodeId);
			msgToSend.typeofquery=("@");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
			Log.e("my", "Query(*):query all avd msg sent:" + msgToSend.toString());

			synchronized (this ) {
				CountQueryAll = 0;
			}
			while(CountQueryAll < RepliesExpected ){
				Log.e("my", "Query(*):waiting for filekeys"+CountQueryAll);
			}

			for(Map.Entry<String,HashMap<String,String>> entry:MapQueryAll.entrySet())
			{
				String emulatorID = (String)entry.getKey();
				HashMap<String,String> fileMap = entry.getValue();
				if(fileMap!=null)
				{
					for(Map.Entry<String,String> entry2:fileMap.entrySet())
					{
						String fileKey=(String)entry2.getKey();
						String fileValue=(String)entry2.getValue();
						matrixcursor.addRow(new String[]{fileKey, fileValue});
					}
				}
			}


			MapQueryAll = null;
			Log.i("my", "query(*):end");
			return matrixcursor;

		} else {
			Log.i("my", "query(direct):start");
			String filekey = selection;
			String filekeyHash = null;
			String senderNodeId = null;
			if(projection!=null) {
				senderNodeId = projection[0];
				StringBuffer fileData=new StringBuffer("");
				try {
					FileInputStream fis = getContext().openFileInput(filekey);
					byte[] buffer = new byte[1024];
					int noOfBytesRead=0;
					while((noOfBytesRead= fis.read(buffer)) != -1 ) {
						fileData.append(new String(buffer, 0, noOfBytesRead));
					}
				} catch (IOException e) {
					Log.e("my", "Query(direct):exceptio:"+e.toString());
				}
				MatrixCursor matrixcursor = new MatrixCursor(new String[] { "key", "value"});
				matrixcursor.addRow(new String[]{filekey, fileData.toString()});
				Log.i("my", "Query(direct):Filekey: "+filekey+" & FileData: "+fileData.toString());
				return matrixcursor;
			}
			try {
				filekeyHash = genHash(filekey);
			} catch(NoSuchAlgorithmException e ) {
				Log.e("my", "In Insert() : No Such Algorithm exception caught ");
				e.printStackTrace();
			}

			int indexthisNode = nodesList.indexOf(thisNodeIdGenHash);
			int indexforKey = 0;
			if(((filekeyHash.compareTo(nodesList.get(4)))>0) ||(filekeyHash.compareTo(nodesList.get(0))<=0)) {
				indexforKey= 0;
			} else {
				for (int i =1 ; i<nodesList.size(); i++) {
					if( (filekeyHash.compareTo(nodesList.get(i-1))>0) && (filekeyHash.compareTo(nodesList.get(i))<=0) ) {
						indexforKey= i;
						break;
					}
				}
			}
			StringBuffer fileData = new StringBuffer("");
			String originalNode = SenderHashMap.get(nodesList.get(indexforKey));
			String successor1= SenderHashMap.get(nodesList.get((indexforKey+1)%5));
			String successor2= SenderHashMap.get(nodesList.get((indexforKey+2)%5));
			if(indexthisNode == indexforKey) {
				boolean flag= true;
				while(flag) {
					try {
						FileInputStream is = getContext().openFileInput(filekey);
						byte[] buffer = new byte[1024];
						int noOfBytesRead=0;
						while((noOfBytesRead= is.read(buffer)) != -1 || fileData.toString()=="") {
							fileData.append(new String(buffer, 0, noOfBytesRead));
						}
						flag= false;
					}catch (IOException e) {
						Log.e("my", "Query(direct):Exception:"+e.toString());
					}
				}
			} else {
				MapQuery= new HashMap<String, String>();
				Message msgToSend = new Message();
				msgToSend.typeofquery=(filekey);
				msgToSend.msgType=("8");
				msgToSend.ReceiverForMsg=(originalNode);
				msgToSend.successor1=(successor1);
				msgToSend.successor2=(successor2);
				msgToSend.senderNodeID=(thisNodeId);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
				Log.e("my", "QUERY_FILE send to Client task with msg : " + msgToSend.toString());

				while(MapQuery.get(filekey)==null){

				}
				fileData.append(MapQuery.get(filekey));
			}
			MatrixCursor matrixcursor = new MatrixCursor(new String[] { "key", "value"});
			matrixcursor.addRow(new String[]{filekey, fileData.toString()});
			Log.i("my", "Query(direct):Filekey: "+filekey+" & File Data : "+fileData.toString());
			Log.i("my", "query(direct):end");
			return matrixcursor;
		}
	}




	public HashMap<String, String> readOrigFiles(String senderGenHash) {
		Log.i("my", "readOrigFiles():start");

		HashMap<String, String> MapFile = new HashMap<String, String>();
		int originalIndexOfFile =0;
		String filekeyHash = null;
		String fileList[] = getContext().fileList();
		for (int cnt= 0; cnt< fileList.length; cnt++) {
			String filekey = fileList[cnt];
			try {
				filekeyHash= genHash(filekey);
			} catch(NoSuchAlgorithmException e ) {
				Log.e("my", "readOrigFiles():exception:"+e.toString());
			}
			if(((filekeyHash.compareTo(nodesList.get(4)))>0) ||(filekeyHash.compareTo(nodesList.get(0))<=0)) {
				originalIndexOfFile= 0;
			} else {
				for (int i =1 ; i<nodesList.size(); i++) {
					if( (filekeyHash.compareTo(nodesList.get(i-1))>0) && (filekeyHash.compareTo(nodesList.get(i))<=0) ) {
						originalIndexOfFile= i;
						break;
					}
				}
			}
			if(nodesList.indexOf(senderGenHash) == originalIndexOfFile) {
				int noOfBytesRead = 0;
				StringBuffer fileData = new StringBuffer("");
				try {
					FileInputStream is = getContext().openFileInput(filekey);
					byte[] buffer = new byte[1024];
					while ((noOfBytesRead= is.read(buffer)) != -1) {
						fileData.append(new String(buffer, 0, noOfBytesRead));
					}
				} catch (Exception e) {
					Log.e("my", "readOrigFiles():Exception:" + e.toString());
				}
				MapFile.put(filekey, fileData.toString());
				Log.i("my", "readOrigFiles():filekey" + filekey+ " with Data: " + fileData.toString());
			}
		}
		Log.i("my", "readOrigFiles():end");
		return MapFile;
	}

	public HashMap<String, String> readReplicatedFiles(String Pred1Hash, String Pred2Hash) {
		Log.i("my","readReplicatedFiles():start");

		HashMap<String, String> MapFile = new HashMap<String, String>();
		String filekey = null;
		int indexPred1Hash = nodesList.indexOf(Pred1Hash);
		int indexPred2Hash = nodesList.indexOf(Pred2Hash);
		int originalIndex =0;
		String filekeyHash = null;

		String fileList[] = getContext().fileList();
		for (int cnt = 0; cnt < fileList.length; cnt++) {
			filekey = fileList[cnt];
			try {
				filekeyHash = genHash(filekey);

			} catch(NoSuchAlgorithmException e ) {
				Log.e("my", "In Delete() For filename : No Such Algorithm exception caught ");
				e.printStackTrace();
			}
			if(((filekeyHash.compareTo(nodesList.get(4)))>0) ||(filekeyHash.compareTo(nodesList.get(0))<=0)) {
				originalIndex= 0;
			} else {
				for (int i =1 ; i<nodesList.size(); i++) {
					if( (filekeyHash.compareTo(nodesList.get(i-1))>0) && (filekeyHash.compareTo(nodesList.get(i))<=0) ) {
						originalIndex= i;
						break;
					}
				}
			}
			if(indexPred1Hash == originalIndex|| indexPred2Hash== originalIndex) {
				int noOfBytesRead = 0;
				StringBuffer fileData = new StringBuffer("");
				try {
					FileInputStream is = getContext().openFileInput(filekey);
					byte[] buffer = new byte[1024];
					while ((noOfBytesRead = is.read(buffer)) != -1) {
						fileData.append(new String(buffer, 0, noOfBytesRead));
					}
				} catch (Exception e) {
					Log.e("my", "readReplicatedFile(): exception" + e.toString());
				}
				MapFile.put(filekey, fileData.toString());
				Log.i("my", "readReplicatedFiles():Filekey: " + filekey+ ": filedata" + fileData.toString());
			}
		}
		Log.i("my","readReplicatedFiles():end");
		return MapFile;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			while (true) {
				Socket socket = null;
				ObjectInputStream is = null;
				try {
					socket = serverSocket.accept();
					is = new ObjectInputStream(socket.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;

					if (RcvdmsgType.equalsIgnoreCase("1")) {
						Log.i("my", "ServerTask(1):start");
						ObjectOutputStream oos = new ObjectOutputStream((socket.getOutputStream()));
						Message msg = new Message();
						msg.msgType=("3");
						msg.senderNodeID=(thisNodeId);
						oos.writeObject(msg);
						oos.flush();

						String key   = (String)RcvdMsg.ContentValues.get("key");
						String value = (String)RcvdMsg.ContentValues.get("value");
						try {
							FileOutputStream os= getContext().openFileOutput(key, MODE_PRIVATE);
							os.write(value.getBytes());
							os.flush();
							os.close();
						} catch (Exception e) {
							Log.e("my", "ServerTask(1):File write failed");
						}
						Log.i("my", "ServerTask(1):end");
					} else if (RcvdmsgType.equalsIgnoreCase("2")) {
						Log.i("my", "ServerTask(2):start");
						ObjectOutputStream oos = new ObjectOutputStream((socket.getOutputStream()));
						Message msg = new Message();
						msg.msgType=("3");
						msg.senderNodeID=(thisNodeId);
						oos.writeObject(msg);
						oos.flush();

						String key   = (String)RcvdMsg.ContentValues.get("key");
						String value = (String)RcvdMsg.ContentValues.get("value");
						FileOutputStream os;
						try {
							os= getContext().openFileOutput(key, MODE_PRIVATE); // check the value of key & Value to be inserted is correct
							os.write(value.getBytes());
							os.flush();
							os.close();
						} catch (Exception e) {
							Log.e("my", "ServerTask(2):File write failed");
						}
						Log.i("my", "ServerTask(2):end");
					} else if (RcvdmsgType.equalsIgnoreCase("5")) {
						Log.i("my", "ServerTask(5):start");
						ObjectOutputStream oos = new ObjectOutputStream((socket.getOutputStream()));
						Message msg = new Message();
						msg.msgType=("7");
						msg.senderNodeID=(thisNodeId);
						oos.writeObject(msg);
						oos.flush();

						delete(myUri, RcvdMsg.typeofquery, new String[]{"5", RcvdMsg.senderNodeID});

						Log.i("my", "ServerTask(5):end");
					} else if (RcvdmsgType.equalsIgnoreCase("4")) {
						Log.i("my", "ServerTask(4):start");
						ObjectOutputStream oos = new ObjectOutputStream((socket.getOutputStream()));
						Message msg = new Message();
						msg.msgType=("7");
						msg.senderNodeID=(thisNodeId);
						oos.writeObject(msg);
						oos.flush();

						delete(myUri, RcvdMsg.typeofquery, new String[]{"4", RcvdMsg.senderNodeID});
						Log.i("my", "ServerTask(4):end");
					} else if (RcvdmsgType.equalsIgnoreCase("6")) {
						Log.i("my", "ServerTask(6):start");
						ObjectOutputStream oos = new ObjectOutputStream((socket.getOutputStream()));
						Message msg = new Message();
						msg.msgType=("7");
						msg.senderNodeID=(thisNodeId);
						oos.writeObject(msg);
						oos.flush();
						delete(myUri, RcvdMsg.typeofquery, new String[]{"6", RcvdMsg.senderNodeID});
						Log.i("my", "ServerTask(6):end");
					} else if (RcvdmsgType.equalsIgnoreCase("8")) {
						Log.i("my", "ServerTask(8):start");

						Cursor resultCursor = query(myUri, new String[]{RcvdMsg.senderNodeID}, RcvdMsg.typeofquery, null, null);
						if (resultCursor == null || resultCursor.getCount()==0) {
							Log.e("my", "ServerTask(8):Result is null");
						} else {
							int keyIndex = 0, valueIndex=0;
							keyIndex = resultCursor.getColumnIndex("key");
							valueIndex = resultCursor.getColumnIndex("value");
							if (keyIndex == -1 || valueIndex == -1) {
								resultCursor.close();
							}
							resultCursor.moveToFirst();
							if (!(resultCursor.isFirst() && resultCursor.isLast())) {
								resultCursor.close();
							}
							String returnFilekey = resultCursor.getString(keyIndex);
							String returnFileData = resultCursor.getString(valueIndex);

							if(returnFileData!=null && returnFileData!="") {


								Message msg = new Message();
								msg.msgType="9";
								msg.ReceiverForMsg=(RcvdMsg.senderNodeID);
								msg.ContentValues.put("key",returnFilekey);
								msg.ContentValues.put("value", returnFileData);
								new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
								Log.i("my", "ServerTask(8):QUERY reply sent to Client:" + msg.toString());
							}
						}
						Log.i("my", "ServerTask(8):end");
					} else if (RcvdmsgType.equalsIgnoreCase("9")) {
						Log.i("my", "ServerTask(9):start");

						MapQuery.put((String)RcvdMsg.ContentValues.get("key"), (String)RcvdMsg.ContentValues.get("value"));
						Log.i("my", "ServerTask(9):end");
					}else if (RcvdmsgType.equalsIgnoreCase("10")) {
						Log.i("my", "ServerTask(10):start");
						String[] queryAllMsg = new String[3];
						queryAllMsg[0] = RcvdmsgType;
						queryAllMsg[1] = RcvdMsg.senderNodeID;
						queryAllMsg[2] = RcvdMsg.typeofquery;



						String senderNodeId = RcvdMsg.senderNodeID;;
						String selectionParam   = RcvdMsg.typeofquery;
						int indexKey = 0, indexvalue=0;
						Cursor cursor = query(myUri, new String[]{"10",senderNodeId}, selectionParam, null, null);
						if (cursor== null || cursor.getCount()<=0) {

							Message msg = new Message();
							msg.msgType=("11");
							msg.ContentValues=null;
							msg.ReceiverForMsg=(senderNodeId);
							msg.senderofrepplyingall=(thisNodeId);
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
						} else {
							indexKey = cursor.getColumnIndex("key");
							indexvalue = cursor.getColumnIndex("value");
							if (indexKey == -1 || indexvalue == -1) {
								cursor.close();
							}
							cursor.moveToFirst();

							String filekey = null;
							String fileData = null;

							HashMap<String,String> fileMap = new HashMap<String,String>();
							filekey= cursor.getString(indexKey);
							fileData= cursor.getString(indexvalue);
							if(fileData!=null && fileData!="") {
								fileMap.put(filekey,fileData);
							}
							while(cursor.moveToNext()) {
								filekey= cursor.getString(indexKey);
								fileData = cursor.getString(indexvalue);
								if(fileData!=null && fileData!="") {
									fileMap.put(filekey,fileData);
								}
							}
							Message msg = new Message();
							msg.msgType=("11");
							msg.ContentValues=(fileMap);
							msg.senderofrepplyingall=(thisNodeId);
							msg.ReceiverForMsg=(senderNodeId);
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
						}
						Log.i("my", "ServerTask(10):end");
					}else if (RcvdmsgType.equalsIgnoreCase("11")) {
						Log.i("my", "ServerTask(11):start");

						MapQueryAll.put(RcvdMsg.senderofrepplyingall,(HashMap)RcvdMsg.ContentValues);
						CountQueryAll++;
						Log.i("my", "ServerTask(11):end");
					} else if (RcvdmsgType.equalsIgnoreCase("13")) {
						Log.i("my", "ServerTask(13):start");
						String senderOfMsg = RcvdMsg.senderNodeID;
						String receiverOfMsg = RcvdMsg.ReceiverForMsg;
						String senderGenHash = null;
						try {
							senderGenHash = genHash(senderOfMsg);
						} catch (NoSuchAlgorithmException e) {
							Log.e("my", "ServerTask(13):Eception:"+e.toString());
						}


						HashMap<String,String> originalFilesMap = readOrigFiles(senderGenHash);
						Message msg = new Message();
						msg.msgType=("14");
						msg.ReceiverForMsg=(senderOfMsg);
						msg.senderNodeID=(receiverOfMsg);
						msg.ContentValues=(originalFilesMap);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
						Log.i("my", "ServerTask(13):end");
					} else if (RcvdmsgType.equalsIgnoreCase("12")) {
						Log.i("my", "ServerTask(12):start");
						String immediatePredec1 = RcvdMsg.ReceiverForMsg;
						String immediatePredec2 = RcvdMsg.successor1;
						String immediatePredec1GenHash = null;
						String immediatePredec2GenHash = null;
						try {
							immediatePredec1GenHash = genHash(immediatePredec1);
							immediatePredec2GenHash = genHash(immediatePredec2);
						} catch (NoSuchAlgorithmException e) {
							Log.e("my", "ServerTask(12):Eception:"+e.toString());
						}


						HashMap<String,String> replicatedFilesMap = readReplicatedFiles(immediatePredec1GenHash,immediatePredec2GenHash);
						Message msg = new Message();
						msg.msgType=("15");
						msg.ReceiverForMsg=(RcvdMsg.senderNodeID);
						msg.senderNodeID=(RcvdMsg.ReceiverForMsg);
						msg.ContentValues=(replicatedFilesMap);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
						Log.i("my", "ServerTask(12):end");
					}  else if (RcvdmsgType.equalsIgnoreCase("14")) {
						Log.i("my", "ServerTask(14):start");
						HashMap<String,String> fileMap = RcvdMsg.ContentValues;
						for(Map.Entry<String,String> entry2: fileMap.entrySet())
						{
							String fileKey=(String)entry2.getKey();
							String fileData=(String)entry2.getValue();
							FileOutputStream outputStream;
							try {
								outputStream = getContext().openFileOutput(fileKey, MODE_PRIVATE);
								outputStream.write(fileData.getBytes());
								outputStream.flush();
								outputStream.close();
							} catch (Exception e) {
								Log.e("my", "ServerTask(14):Exception:"+e.toString());
							}
						}
						Log.i("my", "ServerTask(14):end");
					}  else if (RcvdmsgType.equalsIgnoreCase("15")) {
						Log.i("my", "ServerTask(15):start");
						HashMap<String,String> fileMap = RcvdMsg.ContentValues;
						for(Map.Entry<String,String> entry2:fileMap.entrySet())
						{
							String fileKey=(String)entry2.getKey();
							String fileData=(String)entry2.getValue();
							FileOutputStream outputStream;
							try {
								outputStream = getContext().openFileOutput(fileKey, MODE_PRIVATE);
								outputStream.write(fileData.getBytes());
								outputStream.flush();
								outputStream.close();
							} catch (Exception e) {
								Log.e("my", "ServerTask(15):Exception:"+e.toString());
							}
						}
						Log.i("my", "ServerTask(15):end");
					}
				} catch (IOException e) {
					Log.e("my","ServerTask():Exception:"+e.toString());

				} catch (Exception e) {
					Log.e("my","ServerTask():Exception:"+e.toString());
				}
			}
		}
		protected void onProgressUpdate(String... strings) {

			String msgTypeReceived = strings[0];

		}
	}


	private class ClientTask extends AsyncTask<Message, Void, Void> {

		@Override
		protected Void doInBackground(Message... msgs) {

			Message msg = msgs[0];
			if (msg.msgType.equalsIgnoreCase("1"))  {
				try {
					Log.e("my", "ClientTask() : Sending INSERT_ALL msg to Original Receiver : " + msg.ReceiverForMsg);
					int ReceiverPort1 = Integer.parseInt(msg.ReceiverForMsg) * 2;
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort1);
					ObjectOutputStream os = new ObjectOutputStream((socket1.getOutputStream()));
					os.writeObject(msg);
					os.flush();

					socket1.setSoTimeout(1500);
					ObjectInputStream is = new ObjectInputStream(socket1.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;

				} catch (SocketTimeoutException e) {
					Log.e("my", "ClientTask(1):socket timeoutException");
					String failedDevice = msg.ReceiverForMsg;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(1):Exception:"+e.toString());
				}

				try{
					int ReceiverPort2 = Integer.parseInt(msg.successor1)*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
					socket2.setSoTimeout(1500);

					ObjectInputStream is = new ObjectInputStream(socket2.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;
				}catch (SocketTimeoutException e) {
					Log.e("my", "ClientTask(1): socket timeoutException:"+e.toString());
					String failedDevice = msg.successor1;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(1):Exception:"+e.toString());
				}
				try {
					Log.e("my", "ClientTask(1) : Sending INSERT_ALL msg to Successor 2 : " + msg.successor2);
					int ReceiverPort3 = Integer.parseInt(msg.successor2)*2;
					Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort3);
					ObjectOutputStream os3 = new ObjectOutputStream((socket3.getOutputStream()));
					os3.writeObject(msg);
					os3.flush();

					socket3.setSoTimeout(1500);

					ObjectInputStream is = new ObjectInputStream(socket3.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;
				}  catch (SocketTimeoutException e) {
					String failedDevice = msg.successor2;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(1):Exception:"+e.toString());
				}
			} else if (msg.msgType.equalsIgnoreCase("2"))  {
				try {

					int ReceiverPort2 = Integer.parseInt(msg.successor1)*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
					socket2.setSoTimeout(1500);
					ObjectInputStream is = new ObjectInputStream(socket2.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;
				} catch (SocketTimeoutException e) {
					Log.e("my", "ClientTask(2): socket timeoutException:"+e.toString());
					String failedDevice = msg.successor1;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(2):Exception:"+e.toString());
				}
				try {
					int ReceiverPort3 = Integer.parseInt(msg.successor2)*2;
					Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort3);
					ObjectOutputStream os3 = new ObjectOutputStream((socket3.getOutputStream()));
					os3.writeObject(msg);
					os3.flush();
					socket3.setSoTimeout(1500);

					ObjectInputStream is = new ObjectInputStream(socket3.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;
				} catch (SocketTimeoutException e) {
					String failedDevice = msg.successor2;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(2):Exception:"+e.toString());
				}
			} else if (msg.msgType.equalsIgnoreCase("5"))  {
				try {

					int ReceiverPort1 = Integer.parseInt(msg.ReceiverForMsg)*2;
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort1);
					ObjectOutputStream os = new ObjectOutputStream((socket1.getOutputStream()));
					os.writeObject(msg);
					os.flush();
					socket1.setSoTimeout(1500);

					ObjectInputStream is = new ObjectInputStream(socket1.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;
				} catch (SocketTimeoutException e) {
					String failedDevice= msg.ReceiverForMsg;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask!5):Exceptio:"+e.toString());
				}
				try {
					int ReceiverPort2 = Integer.parseInt(msg.successor1)*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
					socket2.setSoTimeout(1500);

					ObjectInputStream is = new ObjectInputStream(socket2.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;
				} catch (SocketTimeoutException e) {
					String failedDevice = msg.successor1;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(5):Exception:"+e.toString());
				}
				try {
					int ReceiverPort3 = Integer.parseInt(msg.successor2)*2;
					Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort3);
					ObjectOutputStream os3 = new ObjectOutputStream((socket3.getOutputStream()));
					os3.writeObject(msg);
					os3.flush();
					socket3.setSoTimeout(1500);

					ObjectInputStream is = new ObjectInputStream(socket3.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;
				} catch (SocketTimeoutException e) {
					String failedDevice = msg.successor2;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(5):Exception :"+e.toString());
				}
			} else if (msg.msgType.equalsIgnoreCase("4"))  {
				try {

					int ReceiverPort2 = Integer.parseInt(msg.successor1)*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
					socket2.setSoTimeout(1500);

					ObjectInputStream is = new ObjectInputStream(socket2.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;

				} catch (SocketTimeoutException e) {
					String failedDevice = msg.successor1;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(4):Exception :"+e.toString());
				}
				try {
					int ReceiverPort3 = Integer.parseInt(msg.successor2)*2;
					Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort3);
					ObjectOutputStream os3 = new ObjectOutputStream((socket3.getOutputStream()));
					os3.writeObject(msg);
					os3.flush();
					socket3.setSoTimeout(1500);

					ObjectInputStream is = new ObjectInputStream(socket3.getInputStream());
					Message RcvdMsg = (Message) is.readObject();
					String RcvdmsgType = RcvdMsg.msgType;

				}  catch (SocketTimeoutException e) {
					String failedDevice= msg.successor2;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(4):Exception:"+e.toString());
				}
			} else if (msg.msgType.equalsIgnoreCase("6"))  {
				for (String tempport : Ports) {
					try {
						if (!tempport.equals(thisPort)) {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(tempport));
							ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
							os.writeObject(msg);
							os.flush();
							socket.setSoTimeout(1500);
							ObjectInputStream is = new ObjectInputStream(socket.getInputStream());
							Message RcvdMsg = (Message) is.readObject();
							String RcvdmsgType = RcvdMsg.msgType;
						}
					} catch (SocketTimeoutException e) {
						String failedDevice= (Integer.parseInt(tempport))/2+"";
						if(MapQueryAll != null) {
							if(MapQueryAll.get(failedDevice)==null) {
								CountQueryAll = CountQueryAll + 1;
							}
						}
					} catch (Exception e) {
						Log.e("my", "Clienttask(6):Exception:"+e.toString());
					}
				}
			} else if (msg.msgType.equalsIgnoreCase("8"))  {
				try {
					int ReceiverPort2 = Integer.parseInt(msg.ReceiverForMsg) * 2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
				}  catch (SocketTimeoutException e) {
					String failedDevice = msg.ReceiverForMsg;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(8):Exception:"+e.toString());
				}
				try {

					int ReceiverPort1 = Integer.parseInt(msg.successor1)*2;
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort1);
					ObjectOutputStream os1 = new ObjectOutputStream((socket1.getOutputStream()));
					os1.writeObject(msg);
					os1.flush();
				} catch (SocketTimeoutException e) {
					String failedDevice= msg.successor1;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(8):Exception :"+e.toString());
				}
				try {
					int ReceiverPort3 = Integer.parseInt(msg.successor2)*2;
					Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort3);
					ObjectOutputStream os3 = new ObjectOutputStream((socket3.getOutputStream()));
					os3.writeObject(msg);
					os3.flush();
				} catch (SocketTimeoutException e) {
					String failedDevice = msg.successor2;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}

				} catch (Exception e) {
					Log.e("my", "Clienttask(8):Exception :"+e.toString());
				}
			} else if (msg.msgType.equalsIgnoreCase("9"))  {
				try {
					int ReceiverPort2 = Integer.parseInt(msg.ReceiverForMsg)*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
				} catch (SocketTimeoutException e) {
					String failedDeivce= msg.ReceiverForMsg;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDeivce)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(9):Exception :"+e.toString());
				}
			} else if (msg.msgType.equalsIgnoreCase("10"))  {
				for (String tempport : Ports) {
					try {
						if (tempport.equals(thisPort)) {
							continue;
						} else {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(tempport));
							ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
							os.writeObject(msg);
							os.flush();
						}
					} catch (SocketTimeoutException e) {
						String failedDevice = (Integer.parseInt(tempport)/2)+"";
						if(MapQueryAll == null) {
							if(MapQueryAll.get(failedDevice)!=null) {
								CountQueryAll = CountQueryAll + 1;
							}
						}
					} catch (Exception e) {
						Log.e("my", "Clienttask(10):Exception :"+e.toString());
					}
				}
			}  else if (msg.msgType.equalsIgnoreCase("11"))  {
				try {
					int ReceiverPort2 = Integer.parseInt(msg.ReceiverForMsg)*2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
				} catch (SocketTimeoutException e) {

					String failedDevice = msg.ReceiverForMsg;
					if(MapQueryAll != null) {
						if(MapQueryAll.get(failedDevice)==null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(11):Exception :"+e.toString());
				}
			} else if (msg.msgType.equalsIgnoreCase("13")) {
				try {
					int ReceiverPort2 = Integer.parseInt(msg.ReceiverForMsg) * 2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
				} catch (SocketTimeoutException e) {
					String failedDevice = msg.ReceiverForMsg;
					if (MapQueryAll != null) {
						if (MapQueryAll.get(failedDevice) == null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(13):Exception:"+e.toString());
				}
			} else if (msg.msgType.equalsIgnoreCase("12")) {
				try {
					int ReceiverPort2 = Integer.parseInt(msg.ReceiverForMsg) * 2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
				} catch (SocketTimeoutException e) {
					String failedDevice = msg.ReceiverForMsg;
					if (MapQueryAll != null) {
						if (MapQueryAll.get(failedDevice) == null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(12):Exception :"+e.toString());
				}
			} else if (msg.msgType.equalsIgnoreCase("14")) {
				try {
					int ReceiverPort2 = Integer.parseInt(msg.ReceiverForMsg) * 2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
				}  catch (SocketTimeoutException e) {
					String failedDevice = msg.ReceiverForMsg;
					if (MapQueryAll != null) {
						if (MapQueryAll.get(failedDevice) == null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(14):Exception:"+e.toString());
				}
			} else if (msg.msgType.equalsIgnoreCase("15")) {
				try {
					int ReceiverPort2 = Integer.parseInt(msg.ReceiverForMsg) * 2;
					Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							ReceiverPort2);
					ObjectOutputStream os2 = new ObjectOutputStream((socket2.getOutputStream()));
					os2.writeObject(msg);
					os2.flush();
				} catch (SocketTimeoutException e) {
					String failedDevice= msg.ReceiverForMsg;
					if (MapQueryAll != null) {
						if (MapQueryAll.get(failedDevice) == null) {
							CountQueryAll = CountQueryAll + 1;
						}
					}
				} catch (Exception e) {
					Log.e("my", "Clienttask(15):Exception :"+e.toString());
				}
			}
			return null;
		}
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		Log.i("my","genHash():start");
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		Log.i("my","genHash():end");
		return formatter.toString();
	}
}