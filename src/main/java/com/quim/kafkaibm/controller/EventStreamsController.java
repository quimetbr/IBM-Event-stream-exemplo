package com.quim.kafkaibm.controller;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EventStreamsController {
    private KafkaTemplate<String, String> template;
    private List<String> messages = new CopyOnWriteArrayList<>();

    public EventStreamsController(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    @KafkaListener(topics = "${listener.topic}")
    public void listen(ConsumerRecord<String, String> cr) throws Exception {
        messages.add(cr.value());
    }

    
    @GetMapping(value = "send/{msg}")
    //http://localhost:8080/send/Hello
    public String send(@PathVariable String msg) throws Exception {
    	

    	
    	//Mandamos 10 mensagens de vez para gerar volumem na monitoraçao
    	for (int i = 1; i <= 10; ++i) {
    		template.sendDefault(msg+i);
    		//verificando qual partição está sendo utilizada
    		//String partition = template.partitionsFor("topic").toString();
    		//System.out.println("Partition: " + partition);
    	}
    	    	  
    	//apenas 1 mensagem
    	//template.sendDefault(msg);

     	System.out.println("Mensagem enviada ao tópico");
     	String retorno = "Mensagem enviada e recebida: " + msg;
     	return retorno;
    }

    @GetMapping("received")
    //http://localhost:8080/received
    public String recv() throws Exception {
     	System.out.println("Mensagem lida do tópico");
    	String result = messages.toString();
    	//depois de ler apagamos o tópico
        messages.clear();
        return result;
    }
    
    @GetMapping(value = "receive/{offset}")
    //http://localhost:8080/received
    public String recebido(@PathVariable int offset) throws Exception {
       	System.out.println("Mensagem lida do tópico com offset");
       	System.out.println("Quantidade de eventos" + messages.size());
       	String result = messages.get(offset).toString();
     	return result;
    }

}