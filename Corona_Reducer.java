package corona;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;
public class Corona_Reducer extends
Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
/**
* The `Reducer` function. Iterates through all earthquake magnitudes for a
* region to find the maximum value. The output key is the `region name` and
* the value is the `maximum magnitude` for that region.
* @param key - Input key - Name of the region
* @param values - Input Value - Iterator over quake magnitudes for region
* @param context - Used for collecting output
* @throws IOException
* @throws InterruptedException
*/
@Override
public void reduce(Text key, Iterable<DoubleWritable> values,
Context context) throws IOException, InterruptedException {
// Standard algorithm for finding the max value
double deathcount = 0;
for (DoubleWritable value : values) {
deathcount = (deathcount + value.get());
}
context.write(key, new DoubleWritable(deathcount));
}
}