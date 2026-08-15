package org.example;
import java.util.Arrays;

public class test {

    public static void main(String[] args) {
        String values = "Hello World";

        // Correct way: Convert the array to a readable string before printing
        System.out.println(Arrays.toString(values.split(" ")));

    }
}
