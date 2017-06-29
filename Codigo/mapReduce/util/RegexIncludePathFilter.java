/*
 * Classe responsável por filtrar os arquivos de entrada
 */
package mapReduce.util;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexIncludePathFilter implements PathFilter, Configurable{

    String pattern;
    private Configuration conf;
    private FileSystem fs;
    
    @Override
    public boolean accept(Path path) 
    {
        this.pattern = conf.get("pattern");
        
        try 
        {
            if(fs.isDirectory(path))
                return true;
            else
                return path.toString().matches(pattern);
        } catch (IOException ex) {
            return false;
        }
    }
    
    @Override
    public void setConf(Configuration _conf)
    {
        this.conf = _conf;
        try {
            this.fs = FileSystem.get(this.conf);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
    
}
