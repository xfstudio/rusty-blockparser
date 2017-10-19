use std::fs::{self, File};
use std::path::PathBuf;
use std::io::{BufWriter, Write};
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
    r_value:        HashMap<String, String>,
    ww_writer:      BufWriter<File>,

    start_height:   usize,
    end_height:     usize,
    compare_count:  u64,
    weak_count:  u64,
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
                r_value:        HashMap::with_capacity(10000000),
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
        info!(target: "callback", "Using `weakwallets` with dump folder: {} ...", &self.dump_folder.display());
    }

    fn on_block(&mut self, block: Block, block_height: usize) {
        self.end_height = block_height;
        debug!(target: "on_block", "processing...\nDumped {} block:{}\n\
                                   \t-> transactions: {:9}\n\
                                   \t-> inputs:       {:9}\n\
                                   \t-> outputs:      {:9}",
             self.end_height + 1, block.blk_index, self.tx_count, self.in_count, self.out_count);

        for tx in &block.txs {
            let txid_str = utils::arr_to_hex_swapped(&tx.hash);
            for (i, input) in tx.value.inputs.iter().enumerate() {
                let tmp_r = utils::arr_to_hex(&input.script_sig);
                if tmp_r.len() > 74 {
                    self.r_value.insert(txid_str.clone() +  &i.to_string(), tmp_r.clone());
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

        let mut r_arr: Vec<String> = Vec::new();
        for (key, value) in self.r_value.iter() {
            let tmp_sig = &value[10..74].to_string();
            r_arr.push(tmp_sig.clone())
        }

        info!(target: "r_arr.len", "{}", &r_arr.len().to_string());

        for (key, value) in self.r_value.iter() {
            let txid = &key[0..63];
            // let index = &key[64..key.len()-1];
            let tmp_r = value[10..74].to_string();
            // let result = WeakWallets::repeat_r(tmp_r, &r_arr);
            if tmp_r.len() == 64 {
                self.compare_count += 1;
                let n: i8 = 0;
                for r in &r_arr {
                    debug!(target: "repeat_r(tmp_r,r,n)", "{}\t{}\t{}", 
                        tmp_r.to_string(), 
                        r.to_string(), 
                        n.to_string()
                    );
                    if tmp_r.to_string() == r.to_string() {
                        let n = n + 1;
                        if n > 1 {
                            self.weak_count += 1;
                            info!(target: "repeat_r found!(tmp_r,n)", "{}\t{}", 
                                tmp_r, 
                                n.to_string()
                            );
                            self.ww_writer.write_all(format!(
                                "{};{}\n",
                                txid,
                                value
                                ).as_bytes()
                            ).unwrap();
                        }
                    }
                }
            }
            info!(target: "repeat_r", "compare {} found {}", self.compare_count, self.weak_count);
        }

        // Keep in sync with c'tor
        // for f in vec!["blocks", "transactions", "tx_in", "tx_out"] {
        for f in vec!["weak_wallets"] {
            fs::rename(self.dump_folder.as_path().join(format!("{}.csv.tmp", f)),
                       self.dump_folder.as_path().join(format!("{}-{}-{}.csv", f, self.start_height, self.end_height)))
                .expect("Unable to rename tmp file!");
        }

        info!(target: "callback", "Done.\nDumped all {} blocks:\n\
                                   \t-> transactions: {:9}\n\
                                   \t-> inputs:       {:9}\n\
                                   \t-> outputs:      {:9}",
             self.end_height + 1, self.tx_count, self.in_count, self.out_count);
    }
}
