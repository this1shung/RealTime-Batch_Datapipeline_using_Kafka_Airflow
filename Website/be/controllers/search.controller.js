import { fetchFromTMDB } from "../services/tmdb.service.js";
import { User } from "../models/user.model.js";
import { SearchHistory } from "../models/searchHistory.model.js";

export async function searchPerson(req,res){
    const {query} = req.params;
    try {
        const response = await fetchFromTMDB(`https://api.themoviedb.org/3/search/person?query=${query}&include_adult=false&language=en-US&page=1`);

        if(response.results.length === 0){
            return res.status(404).send(null);
        }

        await SearchHistory.create({
            userId: req.user._id,
            id: response.results[0].id,
            image: response.results[0].profile_path,
            title: response.results[0].name,
            searchType: "person",
            keyword: query
        });

        res.status(200).json({success:true,content:response.results})
    } catch (error) {
        console.log("Error in Search Person Controller",error.message);
        res.status(500).json({success:false,message:"Internal Server Error"});
    }
}

export async function searchMovie(req,res){
    const {query} = req.params;

    try {
        const response = await fetchFromTMDB(`https://api.themoviedb.org/3/search/movie?query=${query}&include_adult=false&language=en-US&page=1`);
        
        if(response.results.length === 0){
            return res.status(404).send(null);
        }

        await SearchHistory.create({
            userId: req.user._id,
            id: response.results[0].id,
            image: response.results[0].poster_path,
            title: response.results[0].title,
            searchType: "movie",
            keyword: query
        });
        
        res.status(200).json({success:true,content:response.results})

    } catch (error) {
        console.log("Error in Search Movie Controller",error.message);
        res.status(500).json({success:false,message:"Internal Server Error"});
    }
}

export async function searchTv(req,res){
    const {query} = req.params;

    try {
        const response = await fetchFromTMDB(`https://api.themoviedb.org/3/search/tv?query=${query}&include_adult=false&language=en-US&page=1`);
        
        if(response.results.length === 0){
            return res.status(404).send(null);
        }

        await SearchHistory.create({
            userId: req.user._id,
            id: response.results[0].id,
            image: response.results[0].poster_path,
            title: response.results[0].name,
            searchType: "tv",
            keyword: query
        });

        res.status(200).json({success:true,content:response.results})

    } catch (error) {
        console.log("Error in Search TV Controller",error.message);
        res.status(500).json({success:false,message:"Internal Server Error"});
    }
}


export async function getSearchHistory(req, res) {
    try {
        const searchHistory = await SearchHistory.find({ userId: req.user._id });
        res.status(200).json({ success: true, content: searchHistory });
    } catch (error) {
        res.status(500).json({ success: false, message: "Internal Server Error" });
    }
}


export async function removeItemFromSearchHistory(req, res) {
    const { id } = req.params;
    try {
        await SearchHistory.findOneAndDelete({ userId: req.user._id, id: parseInt(id) });
        res.status(200).json({ success: true, message: "Removed" });
    } catch (error) {
        console.log("Error in Remove Item Function", error.message);
        res.status(500).json({ success: false, message: "Internal Server Error" });
    }
}


// export async function getSearchHistory(req,res){
//     try {
//         res.status(200).json({success:true, content:req.user.searchHistory});
//     } catch (error) {
//         res.status(500).json({success:false,message:"Internal Server Error"});
//     }
// }

// export async function removeItemFromSearchHistory(req,res) {
//     let {id} = req.params;

//     id = parseInt(id);

//     try{
//         await User.findByIdAndUpdate(req.user._id,{
//             $pull:{
//                 searchHistory:{id:id},
//             }
//         });

//         res.status(200).json({success:true,message:"Removed"});
//     }catch(error){
//         console.log("Error in Remove Item Function",error.message);
//         res.status(500).json({success:false,message:"Internal Server Error"});
//     }
// }