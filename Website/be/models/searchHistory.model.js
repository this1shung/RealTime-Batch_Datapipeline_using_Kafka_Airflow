import mongoose from "mongoose";

const searchHistorySchema = new mongoose.Schema({
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: true
  },
  id: {
    type: Number,
    required: true
  },
  image: {
    type: String
  },
  title: {
    type: String,
    required: true
  },
  searchType: {
    type: String,
    required: true,
    enum: ['person', 'movie', 'tv']
  },
  createdAt: {
    type: Date,
    default: Date.now
  },
  keyword: {
    type: String,
    required: true
  }
});

export const SearchHistory = mongoose.model('SearchHistory', searchHistorySchema);
