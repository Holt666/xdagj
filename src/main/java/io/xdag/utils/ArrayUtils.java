package io.xdag.utils;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public final class ArrayUtils {

    /**
     * Generate a random permutation of [0...n)
     */
    public static int[] permutation(int n) {
        int[] arr = new int[n];
        for (int i = 0; i < n; i++) {
            arr[i] = i;
        }

        shuffle(arr);
        return arr;
    }

    /**
     * Shuffle an integer array.
     */
    public static void shuffle(int[] arr) {
        Random r = ThreadLocalRandom.current();

        for (int i = arr.length - 1; i > 0; i--) {
            int index = r.nextInt(i + 1);

            int tmp = arr[index];
            arr[index] = arr[i];
            arr[i] = tmp;
        }
    }

}
