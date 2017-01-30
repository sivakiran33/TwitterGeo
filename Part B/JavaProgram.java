package ttu.edu;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;


public final class JavaPageRank{
  private static final Pattern SPACES = Pattern.compile("\\s+");

  private static class Sum extends Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }
  //This is to print the error message if the arguments are not passed correctly
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: JavaPageRank <master> <file> <number_of_iterations>");
      System.exit(1);
    }

    JavaSparkContext ctx = new JavaSparkContext(args[0], "JavaPageRank",
      System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(JavaPageRank.class));

    // Loads in input file. It should be in format of:
    //     USER         neighbor USER
    //     ...

    JavaRDD<String> lines = ctx.textFile(args[1], 1);

    // Loads all users from input file and initialize their neighbors.
    JavaPairRDD<String, List<String>> links = lines.map(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        String[] parts = SPACES.split(s);
        return new Tuple2<String, String>(parts[0], parts[1]);
      }
    }).distinct().groupByKey().cache();

    // Initializing ranks to the users.
    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<List<String>, Double>() {
      @Override
      public Double call(List<String> rs) {
        return 1.0;
      }
    });

    // Calculating and updating the user ranks using PageRank algorithm.
    for (int iter = 0; iter < Integer.parseInt(args[2]); iter++) {
      // Calculate the USER contributions to the rank of other users.
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
        .flatMap(new PairFlatMapFunction<Tuple2<List<String>, Double>, String, Double>() {
          @Override
          public Iterable<Tuple2<String, Double>> call(Tuple2<List<String>, Double> s) {
            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
            for (String n : s._1()) {
              results.add(new Tuple2<String, Double>(n, s._2() / s._1().size()));
            }
            return results;
          }
      });

      // Recalculate the USER ranks based on neighbor USER contributions.
      ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
        @Override
        public Double call(Double sum) {
          return 0.15 + sum * 0.85;
        }
      });
    }

    // Collect all USER ranks and displays them in console.
    List<Tuple2<String, Double>> output = ranks.collect();
    for (Tuple2<?,?> tuple : output) {
        System.out.println("id:" + tuple._1() + " rank: " + tuple._2());
    }

    //Creates a directory PageRank in the hadoop and stores the output values in file.
    ranks.saveAsTextFile("PageRank");
    System.exit(0);
  }
}
