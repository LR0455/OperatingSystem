#include <omp.h>
#include <bits/stdc++.h>
#include <unistd.h>
using namespace std; 

struct command{
    int client_id;
    string operate;
    vector<string> fields;
};

// varible
// ------------------------------------
int client_num;
    
deque< vector<string> > tuple_space;
deque<command> cmds;

vector<bool> client_active, client_wait;
vector<string> client_value[105];

map<string, string> varible;
omp_lock_t my_lock;
// ------------------------------------

command parse_line(string &line) {
    command cmd;
    istringstream sin(line);
    sin >> cmd.client_id >> cmd.operate;
    string field;
    while(sin >> field)
        cmd.fields.push_back(field);
    return cmd;
}

bool isTupleExist(command cmd){
    vector<bool> isVarible(cmd.fields.size());
    for (int i = 0 ; i < cmd.fields.size() ; i ++)
    {
        if (cmd.fields[i][0] == '?')
            isVarible[i] = true;
        else if (cmd.fields[i][0] != '"' && !isdigit(cmd.fields[i][0]))
            cmd.fields[i] = varible[cmd.fields[i]];
    }
                
    for (int i = 0 ; i < tuple_space.size() ; i ++)
        if (cmd.fields.size() == tuple_space[i].size())
        {
            bool same_flag = true;
            for (int j = 0 ; j < cmd.fields.size() ; j ++)
                if (!isVarible[j] && cmd.fields[j] != tuple_space[i][j])
                    same_flag = false;
                
            if (same_flag == true)
            {
                client_value[cmd.client_id] = tuple_space[i];
                
                for (int j = 0 ; j < cmd.fields.size() ; j ++)
                    if (isVarible[j])
                    {
                        string var = "";
                        for (int k = 1 ; k < cmd.fields[j].size() ; k ++)
                            var += cmd.fields[j][k];
                        varible[var] = tuple_space[i][j];
                    }

                if (cmd.operate == "in")
                    tuple_space.erase(tuple_space.begin() + i);
                client_active[cmd.client_id] = true;
                
                omp_set_lock(&my_lock);
                while(client_active[cmd.client_id]){
                    omp_unset_lock(&my_lock);
                    usleep(1000);
                    omp_set_lock(&my_lock);
                };
                omp_unset_lock(&my_lock);
                #ifdef dbg
                puts("in and read -> client_value output:");
                for (int j = 0  ; j < client_value[cmd.client_id].size() ; j ++)
                    printf("%s ", client_value[cmd.client_id][j].c_str());
                puts("");
                #endif

                return true;
            }
        }   

    return false;
}

string getFileName(int thread_id){
    if (thread_id == 0)
        return "server.txt";

    string filename;
    stringstream ss;
    ss << thread_id;
    ss >> filename;

    ss.str("");
    ss.clear();

    return filename + ".txt";
}
void writeFile(int thread_id, string content, int flag){
    string filename = getFileName(thread_id);
    ofstream fout;
    if (!flag) {
        fout.open(filename.c_str(), ios::binary | ofstream::out | ofstream::app);
        fout << content << endl;
    }
    else
        fout.open(filename.c_str(), ofstream::out | ofstream::trunc);
        
    fout.close();
}
void write_tuple_space_to_file(){
    string content = "(";
    for (int i = 0 ; i < tuple_space.size() ; i ++)
    {                
        content += "(";
        for (int j = 0 ; j < tuple_space[i].size() ; j ++)
        {
            if (j)
                content += ",";
            content += tuple_space[i][j];
        }
        content += ")";
    }
    content += ")";
    writeFile(0, content, 0);
}
int main(){
    puts("please input client number:");
    cin >> client_num;
    client_active.resize(client_num + 1);
    client_wait.resize(client_num + 1);

    omp_init_lock(&my_lock);
    #pragma omp parallel num_threads(client_num + 1)
    {
        int thread_id = omp_get_thread_num();

        writeFile(thread_id, "", 1);
        
        // server thread
        if (thread_id == 0)
        {
            #pragma omp single nowait 
            {
                string line;
                getline(cin, line);
                while (true)
                {
                    puts("please input command:");
                    getline(cin, line);
                    if (line == "exit")
                        exit(0);
                        
                    command cmd = parse_line(line);
                    
                    #ifdef dbg
                    printf("id:%d\noperate:%s\nfield:", cmd.client_id, cmd.operate.c_str());
                    for(int i = 0 ; i < cmd.fields.size() ; i ++)
                        cout << cmd.fields[i] << ' ';
                    puts("\n");
                    #endif

                    if (cmd.operate == "in" || cmd.operate == "read")
                    {
                        if (isTupleExist(cmd) == false)
                        {
                            cmds.push_back(cmd);
                            client_wait[cmd.client_id] = true;
                        }
                        else if (cmd.operate == "in")
                        {
                            write_tuple_space_to_file();
                        }
                    }                 
                    else
                    {
                        if (client_wait[cmd.client_id])
                            continue;

                        for (int i = 0 ; i < cmd.fields.size() ; i ++)
                            if (cmd.fields[i][0] != '"' && !isdigit(cmd.fields[i][0]))
                            {
                                string var = "";
                                for (int j = 0 ; j < cmd.fields[i].size() ; j ++)
                                    var += cmd.fields[i][j];
                                if (varible.count(var) > 0)
                                    cmd.fields[i] = varible[var];
                            }

                        tuple_space.push_back(cmd.fields);

                        for (int i = 0 ; i < cmds.size() ; )
                        {
                            if (isTupleExist(cmds[i]))
                            {
                                cmds.erase(cmds.begin() + i);
                                client_wait[cmds[i].client_id] = false;
                            }
                            else
                            {
                                i ++;
                            }                           
                        } 
                         write_tuple_space_to_file();  
                    }     
                    
                    #ifdef dbg
                    puts("tuple_space output:");
                    for (int i = 0 ; i < tuple_space.size() ; i ++)
                    {
                        printf("%d:", i);
                        for(int j = 0 ; j < tuple_space[i].size() ; j ++)
                            printf("%s ", tuple_space[i][j].c_str());
                        puts("");
                    }
                    puts("");
                    puts("varible output:");
                    for (map<string, string>::iterator iter = varible.begin() ; iter != varible.end() ; iter ++)
                        printf("(%s:%s)", iter->first.c_str(), iter->second.c_str());
                    puts("");

                    puts("cmds output:");
                    for (int i = 0 ; i < cmds.size() ; i ++)
                        printf("%d ", cmds[i].client_id);
                    puts("");
                    #endif
                }
            }
        }
        // client thread
        while (true)
        {
            omp_set_lock(&my_lock);
            if(client_active[thread_id]){               
                string content = "(";
                for (int i = 0 ; i < client_value[thread_id].size() ; i ++)
                {
                    if (i)
                        content += ",";
                    content += client_value[thread_id][i];
                }
                content += ")";
                writeFile(thread_id, content, 0);
                
                client_active[thread_id] = false;
                };
            omp_unset_lock(&my_lock);
        }
        
    }
}