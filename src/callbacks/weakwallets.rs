use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::io::{BufWriter, Write, BufReader};
use std::io::prelude::*;
use std::string::String;
use std::collections::HashMap;

use clap::{Arg, ArgMatches, App, SubCommand};

use callbacks::Callback;
use errors::{OpError, OpResult};
use blockchain::parser::types::CoinType;
use blockchain::proto::block::Block;
use blockchain::utils;

/// Dumps the whole blockchain into csv files
pub struct WeakWallets {
    // Each structure gets stored in a seperate csv file
    dump_folder:    PathBuf,
    temp_folder:    PathBuf,
    r_value:        HashMap<String, String>,
    ww_writer:      BufWriter<File>,

    start_height:   usize,
    end_height:     usize,
    compare_count:  u64,
    weak_count:     u64,
    tx_count:       u64,
    in_count:       u64,
    out_count:      u64
}

impl WeakWallets {
    fn create_writer(cap: usize, path: PathBuf) -> OpResult<BufWriter<File>> {
        let file = match File::create(&path) {
            Ok(f) => f,
            Err(err) => return Err(OpError::from(err))
        };
        Ok(BufWriter::with_capacity(cap, file))
    }

    fn compute_r(sig: String) -> String {
        // sig[10..74].to_string()
        sig[10..15].to_string()
    }

    fn repeat_r(sig: String, arr: &Vec<String>) -> bool {
        debug!(target: "repeat_r(sig)", "{}\t{}", sig, sig.len().to_string());
        // if sig.len() == 64 {
        if sig.len() == 5 {
            let n: i8 = 0;
            for r in arr {
                debug!(target: "repeat_r(sig,r,n)", "{}\t{}\t{}", 
                    sig.to_string(), 
                    r.to_string(), 
                    n.to_string()
                );
                if sig.to_string() == r.to_string() {
                    let n = n + 1;
                    if n > 1 {
                        info!(target: "repeat_r found!(sig,n)", "{}\t{}", 
                            sig, 
                            n.to_string()
                        );
                        return true
                    }
                }
            }
        }
       false
    }

    fn fread(filename: PathBuf) -> Vec<String> {
        let file = File::open(filename).unwrap();
        let fin = BufReader::new(file);
        let mut arr = Vec::new();
      
        for line in fin.lines() {
            arr.push(line.unwrap());
        }
        arr
    }

    fn fwrite(filename: PathBuf, con: String) {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(filename)
            .unwrap();

        if let Err(e) = writeln!(file, "{}", con) {
            println!("{}", e);
        }
    }

}

impl Callback for WeakWallets {

    fn build_subcommand<'a, 'b>() -> App<'a, 'b> where Self: Sized {
        SubCommand::with_name("weakwallets")
            .about("Dumps the weak wallets into CSV files")
            .version("0.1")
            .author("windy <xfstudio@qq.com>")
            .arg(Arg::with_name("dump-folder")
                .help("Folder to store csv files")
                .index(1)
                .required(true))
    }

    fn new(matches: &ArgMatches) -> OpResult<Self> where Self: Sized {
        let ref dump_folder = PathBuf::from(matches.value_of("dump-folder").unwrap()); // Save to unwrap
        match (|| -> OpResult<Self> {
            let cap = 4000000;
            let cb = WeakWallets {
                dump_folder:    PathBuf::from(dump_folder),
                temp_folder:    dump_folder.join("temp"),
                r_value:        HashMap::with_capacity(cap),
                ww_writer:      try!(WeakWallets::create_writer(cap, dump_folder.join("weak_wallets.csv.tmp"))),
                start_height: 0, end_height: 0, compare_count: 0, weak_count: 0, tx_count: 0, in_count: 0, out_count: 0
            };
            Ok(cb)
        })() {
            Ok(s) => return Ok(s),
            Err(e) => return Err(
                tag_err!(e, "Couldn't initialize weakwallets with folder: `{}`", dump_folder
                        .as_path()
                        .display()))
        }
    }

    fn on_start(&mut self, _: CoinType, block_height: usize) {
        self.start_height = block_height;
        fs::create_dir(&self.temp_folder)
            .expect("create rvalue temp dirictory faild");
        info!(target: "callback", "Using `weakwallets` with dump folder: {} ...", &self.dump_folder.display());
    }

    fn on_block(&mut self, block: Block, block_height: usize) {
        self.end_height = block_height;

        let block_index = &block.blk_index;
        for tx in &block.txs {
            let txid_str = utils::arr_to_hex_swapped(&tx.hash);
            for input in tx.value.inputs.iter() {
                let tmp_r = utils::arr_to_hex(&input.script_sig);
                if tmp_r.len() > 74 {
                    self.r_value.insert(txid_str.clone() +  &block_index.to_string(), tmp_r.clone());
                    let filepath = self.temp_folder.as_path().join(&block_index.to_string());
                    WeakWallets::fwrite(filepath, WeakWallets::compute_r(tmp_r));
                }
            }
            self.in_count += tx.value.in_count.value;
            self.out_count += tx.value.out_count.value;
        }
        self.tx_count += block.tx_count.value;

    }

    fn on_complete(&mut self, block_height: usize) {
        self.end_height = block_height;
        
        self.ww_writer.write_all(format!(
            "{};{}\n",
            "txid",
            "sig"
            ).as_bytes()
        ).unwrap();

        for (key, value) in self.r_value.iter() {
            let txid = &key[0..64];
            let block_index = &key[64..key.len()];
            let filepath = self.temp_folder.as_path().join(&block_index.to_string());
            debug!(target:"on_complete(txid, block_index, filepath)", "{}\t{}\t{}", txid, block_index, filepath.display().to_string());
            let r_arr = WeakWallets::fread(filepath);
            let tmp_r = WeakWallets::compute_r(value.to_string());
            let result = WeakWallets::repeat_r(tmp_r, &r_arr);
            self.compare_count += 1;
            if result {
                self.ww_writer.write_all(format!(
                    "{};{}\n",
                    txid,
                    value
                    ).as_bytes()
                ).unwrap();
                self.weak_count += 1;
            }
        }
        info!(target: "repeat_r", "compare {} found {}", self.compare_count, self.weak_count);
        // Keep in sync with c'tor
        // for f in vec!["blocks", "transactions", "tx_in", "tx_out"] {
        for f in vec!["weak_wallets"] {
            fs::rename(self.dump_folder.as_path().join(format!("{}.csv.tmp", f)),
                       self.dump_folder.as_path().join(format!("{}-{}-{}.csv", f, self.start_height, self.end_height)))
                .expect("Unable to rename tmp file!");
        }
        fs::remove_dir_all(&self.temp_folder)
            .expect("Unable to remove rvalue tmp dirictory!");

        info!(target: "callback", "Done.\nDumped all {} blocks:\n\
                                   \t-> transactions: {:9}\n\
                                   \t-> inputs:       {:9}\n\
                                   \t-> outputs:      {:9}",
             self.end_height + 1, self.tx_count, self.in_count, self.out_count);
    }
}
