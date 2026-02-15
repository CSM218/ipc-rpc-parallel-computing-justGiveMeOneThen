package pdc;
import java.util.Random;
public class MatrixGenerator {
    private static final Random random = new Random();
    public static int[][] generateRandomMatrix(int rows, int cols, int maxValue) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = random.nextInt(maxValue);
            }
        }
        return matrix;
    }

    public static int[][] generateIdentityMatrix(int size) {
        int[][] matrix = new int[size][size];
        for (int i = 0; i < size; i++) {
            matrix[i][i] = 1;
        }
        return matrix;
    }

    public static int[][] generateFilledMatrix(int rows, int cols, int value) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = value;
            }
        }
        return matrix;
    }

    public static void printMatrix(int[][] matrix, String label) {
        if (label != null && !label.isEmpty()) {
            System.out.println(label);
        }
        for (int[] row : matrix) {
            for (int val : row) {
                System.out.printf("%6d ", val);
            }
            System.out.println();
        }
    }

    public static void printMatrix(int[][] matrix) {
        printMatrix(matrix, "");
    }
}