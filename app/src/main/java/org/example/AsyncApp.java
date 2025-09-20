package org.example;

import java.util.concurrent.CompletableFuture;

public class AsyncApp {
    public String getGreeting(){
        return "Async Hello!";
    }

    public static void main(String[] args) throws Exception{
        CompletableFuture<Void> f = CompletableFuture.runAsync(() -> {
            System.out.println("Async Hello");
        });

        f.get();

        System.out.println("World!");
    }
}
