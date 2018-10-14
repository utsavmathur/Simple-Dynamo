package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by utsav on 5/11/2018.
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class Message implements Serializable{

    public String msgType= null;
    public String senderNodeID = null;
    public ArrayList<String> ListOfAliveNodes = new ArrayList<String>();
    public HashMap ContentValues = new HashMap();
    public String ReceiverForMsg = null;
    public String successor1 = null;
    public String successor2 = null;
    public String typeofquery = null;
    public String senderofrepplyingall = null;


    /*1"insert everything";
    2"insert replicated";
    3"insert ack";

    4"delete replicated";
    5"delete from all avd";
    6"delete all msgs";
    7"delete ack";

    8"query";
    9"query reply";
    10"query all";
    11"query all reply";


    12"recover replicated";
    13"recover original";
    14"send original";
    15"send replicated";*/
}
