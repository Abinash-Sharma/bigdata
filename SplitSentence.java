/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.abinash.example;

/**
 *
 * @author Abinash
 */

import java.text.BreakIterator;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;

public class SplitSentence extends BaseBasicBolt {
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //Get the sentence content from tuple
        String sentence = tuple.getString(0);
        //An Iterator to get each word
        BreakIterator boundary=BreakIterator.getWordInstance();
        //Give Iterator the sentence
        boundary.setText(sentence);
        //Find the begining first word.
        int start=boundary.first();
        //Iterate over each word and emit it to the output stream
        for (int end=boundary.next(); end != BreakIterator.DONE; start=end, end=boundary.next()) {
        //get the word
        String word=sentence.substring(start,end);
        //If a word is whitespace characters, replace it with empty
        word=word.replaceAll("\\s+", "");
        //If it's an actual word. emit it
        if(!word.equals("")) {
            collector.emit(new Values(word));
        }
       }
    }
//Declare that emitted tuples contain a word field 
@Override
public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
 }
}
