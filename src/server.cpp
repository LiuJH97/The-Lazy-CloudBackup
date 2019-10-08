#include<iostream>
#include<string>
#include<unordered_map>
#include<vector>
#include<thread>
#include<fstream>
#include<boost/filesystem.hpp>
#include<boost/algorithm/string.hpp>
#include<unistd.h>
#include<fcntl.h>
#include<zlib.h>
#include"httplib.h"
#include<sys/file.h>
#include<sstream>
#include<sys/stat.h>
#include<pthread.h>
#define SEVER_BASE_DIR "liu"
#define SERVER_ADDR "0.0.0.0"
#define SERVER_PORT 9000
#define SERVER_BACKUP_DIR SERVER_BASE_DIR"/list/"

#define UNGZIPFILE_PATH "liu/list/"
#define GZIPFILE_PATH "liu/zip/"

#define RECORD_FILE "record.list"
#define HEAT_TIME  20
using namespace httplib;
namespace bf = boost::filesystem;

CompressStore cstore;
//服务端压缩存储：
//	压缩存储作用：节省磁盘空间/将多个文件压缩成一个压缩包节省inode节点
//	压缩存储流程：一旦文件压缩存储，服务端下载性能降低
//	因此，对热度较低的文件进行压缩存储，(通过文件最后一次访问时间判断)
//  热度较低的评判：当前时间-文件最后一次访问时间 > 自己规定的时间间隔
//	检测获取/list/目录下的所有文件名
//	通过文件名判断是否需要压缩存储
//	对文件进行压缩存储，删除源文件
//	对文件进行解压缩，向外能够提供正常文件下载
//	需要保存源文件名，与压缩包名称的对应关系

class CompressStore{

private:
	std::string _file_dir;
	std::unordered_map<std::string, std::string> _file_list;
	pthread_rwlock_t _rwlock;
private:
	//每次压缩存储线程启动，从文件读取列表信息
	bool GetListRecord(){
	//每次压缩完毕，都要将列表信息，存储到文件中
	//filename  gzipfilename\n
		bf::path name(RECORD_FILE);
		if(!bf::exists(name)){
			std::cerr << "record file is not exists\n";
			return false;	
		}
		std::ifstream file(RECORD_FILE, std::ios::binary);
		if(!file.is_open()){
			std::cerr << "open record file read error\n";
			return false;
		}
		int64_t fsize = bf::file_size(name);
		std::string body;
		body.resize(fszie);
		file.read(&body[0], fsize);
		if(!file.good()){
			std::cerr << "record file body read error\n"
			return false;
		}
		file.close();
		
		std::vector<std::string> list;
		boost::split(list, body, boost::is_any_of("\n"));
		for(auto i:list){
			//filename gzipname
			size_t pos = i.find(" ");
			if(pos == std::string::npos){
				continue;
			}
			std::string key = i.substr(0, pos);
			std::string val = i.substr(pos+1);
			_file_list[key] = val;
			}
		return true;	
	}
	bool SetListRecord(){
	//获取list目录下文件名称
		std::stringstream tmp;
		for(auto i : _file_list){
			tmp << i->first << " " << i->second << "\n"; 	
		}	
		std::ofstream file(RECORD_FILE, std::ios::binary);
		if(!file.is_open()){
			std::cerr << "record file open error\n";
			return false;
		}
		file.write(tmp.str().c_str(), tmp.str().size());
		if(!file.good()){
			std::cerr << "record file write body error\n";
			return false;
		}
		file.close();
		return true;
	}

	bool IsNeedCompress(std::string &file){
	//判断文件是否需要压缩
		struct stat st;
		if(stat(file.c_str(), &st) < 0){
			std::cerr << "get file:[" << file << "] stat error\n";
			return false;
		}
		time_t cur_time = time(NULL);
		time_t acc_time = st.st_atime;
		if((cur_time - acc_time) < HEAT_TIME){
			return false;
		}
		return true;	
	}
	bool CompressFile(std::string &file, std::string &gzip){
	//对文件进行压缩存储
		int fd = open(file.c_str(), O_RDONLY);
		if(fd < 0){
			std::cerr << "com open file:[" << file << "] error\n";
			return false;
		}
		gzFile gf = gzopen(gzip.c_str(), "wb");
		if(gf == NULL){
			std::cerr << "com open gzip:[" << gzfile << "] error\n";
			return false;
		}
		int ret;
		char buf[1024];
		while((ret = read(fd, buf, 1024)) > 0 ){
			gzwrite(gf, buf, ret);
		}
		close(fd);
		gzclose(gf);
		unlink(file.c_str());
		_file_list[file] = gzfile;
		return true;	
	}
	bool UnCompressFile(std::string &gzip, std::string &file){
		int fd = open(file.c_str(), O_CREAT|O_WRONLY, 0664);
		if(fd < 0){
			std::cerr << "open file" << file << "failed\n";
			return false;
		}
		gzFile gf = gzopen(gzip.c_str(), "rb");
		if(gf == NULL){
			std::cerr << "open gzip" << gzip << "failed\n";
			close(fd);
			return false;
		}
		int ret;
		char buf[1024] = {0};
		while((ret = gzread(bf, buf, 1024 )) > 0){
			int len = write(fd, buf, ret);
			if(len < 0){
				std::cerr << "get gzip data failed\n";
				gzclose(gf);
				close(fd);	
				return false;
			}
		}
		gzclose(gf);
		close(fd);
		unlink(gzip);	
		return;	
	}
	bool GetNormalFile(std::string &name, std::string &body){
		int64_t fsize = bf::file_size(name);
		body.resize(fsize);
		
		std::ifstream file(name, std::ios::binary);
		if(!file.is_open()){
			std::cerr << "open file" << name << "failed\n";
			return false;
		}
		file.read(&body[0], fsize);
		if(!file.good()){
			std::cerr << "get file" << file << "error\n";
			file.close();
			return false;
		}
		file.close();
		return true;
	}
public:
	bool GetFileList(std::vector<std::string> list){	
	//向外提供获取文件数据功能
		pthread_rwlock_rdlock(&_rwlock);
		for(auto i : _file_list){
			list.push_back(i->first);
		}
		pthread_rwlock_unlock(&_rwlock);
		return true;
	}
	bool GetFileGzip(std::string &file, std::string &gzip){
		pthread_rwlock_rdlock(&_rwlock);
		auto it = _file_list.find(file);
		if(it == _file_list.end()){
			pthread_rwlock_unlock(&_rwlock);
			return false;
		}
		std::string gzip = it->second;
		pthread_rwlock_unlock(&_rwlock);
		return true;
	}
	bool GetFileData(std::string &file, std::string &body){
	//非压缩文件数据获取
		if(bf::exists(file)){
			GetNormalFile(file, body);
		}else{
			std::string gzip;
			GetFileGzip(file, gzip);
			UnCompressFile(gzip, file);
			GetNormalFile(file, body);
		}
		return true;
	}
	bool AddFileRecord(std::string file){
		pthread_rwlock_wrlock(&_file_list);
		_file_list[file] = "";
		pthread_rwlock_unlock(&_file_list);	
		return true;
	}
	bool SetFileData(std::string &file, std::string &body, int64_t offset){
		int fd = open(file.c_str(), O_CREAT|O_WRONLY, 0664);
		if(fd < 0){
			std::cerr << "open file" << file << "error\n";
			return false;
		}
		lseek(fd, offset, SEEK_SET);
		int ret = write(fd, &body[0], body.size());
		if(ret < 0){
			std::cerr << "store file" << file << " data error\n";
				return false;
		}
		close(fd);
		AddFileRecord(file);
		return true;
	}
	bool LowDownloadFileStore(){
		GetListRecord();
		//压缩存储流程是死循环，所有需要启动线程进行判断
		while(1){
		//目录检测，热度低的文件进行压缩存储
		//获取list目录下文件名称
		//判断文件是否需要压缩
		//对文件进行压缩存储
			DirectoryCheck();
	    	//存储记录信息
			SetListRecord();
			sleep(10);	
		}
	};
}

class CloudServer
{
private:
	Server srv;
public:
	CloudServer(){
		bf::path base_path(SERVER_BASE_DIR);
		if(!bf::exists(base_path)){
			bf::create_directory(base_path);
		}
		bf::path list_path(SERVER_BAKEUP_DIR);
		if(!bf::exists(list_path)){
			bf::create_directory(list_path);
		}
	}

	bool Start(){
		srv.set_base_dir(SERVER_BASE_DIR);
		srv.Get("/(list(/){0,1}){0,1}",GetFileList);
		srv.Get("/list/(.*)", GetFileData);
		srv.Put("/list/(.*)",PutFileData);
		srv.listen("SERVER_ADDR",SERVER_PORT);
		return true;
	}	

private:
	static void PutFileData(const Request &req, Response &rsp){
		std::cout << "backup file" << req.path << "\n";
		if (!req.has_header("Range")){
			rsp.status = 400;
			return;
		}
		std::string range = req.get_header_value("Range");
		int64_t range_start;
		if(RangeParse(range, range_start) == false){
			rsp.status = 400;
			return;
		}
		std::cout << "backup file:[" << req.path << "] range:[" << range << "]\n";
		std::string realpath = SERVER_BASE_DIR + req.path;
		cstore.SetFileData(realpath, req.body, range_start);
		return;
}
		static bool RangeParse(std::string &range, int64_t &start){
			//bytes = start - end;
			size_t pos1 = range.find("=");
			size_t pos2 = range.find("=");
			if(pos1 == std::string::npos || pos2 == std::string::npos){
				std::cerr << "range:[" << range << "] format error\n";
				return false;
			}
			std::stringstream rs;
			rs << range.substr(pos1+1, pos2 - pos1 - 1);
			rs >> start;
			return true;
		}
		static void GetFileList(const Request &req, Response &rsp){
			std::vector<std::string> list;
			cstore.GetFileList(list);
			std::string body;
			body = "<html><body><ol><hr />";
			for(auto i : list){
				bf::path path(i);
				std::string file = path().filename().string();
				std::string uri = "/list" + file;
				body += "<h4><li>";
				body += "<a href='";
				body += uri;
				body += "'>";
				body += file;
				body += "</a>";
				body += "</li></h4>";
			}
			body += "<hr /></ol></body></html>";
			rsp.set_contect(&body[0], "text/html");
			return;
		}
		static void GetFileData(const Request &req, Response &rsp){
			std::cerr << "into GetFileData \n";
			std::cerr << SERVER_BASE_DIR + req.path;
			if(!bf::exists(file)){
				std::cerr << "file " << file << "is not exists\n";
				rsp.status = 404;
				return;
			}
			std::string body;
			int64_t fsize = bf::file_size(file);
			body.resize(fsize);
			ifile.read(&body[0], fsize);
			if(!ifile.good()){
				std::cerr << "read file " << file << "body error\n";
				rsp.status = 500;
				return;
			}
			rsp.set_content(body, "text/plain");
		}
};

void thr_start(){
	cstor.LowHeatFileStore();
}	
int main()
{
	std::thread thr(thr_start);
	thr.detach();
	CloudServer srv("./cert.pem", "./key.pem");
	srv.Start();
	return 0;	
}	
