import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

interface VersionsInterface extends Model {
    id: number;
    type: number;
    version: string;
}

export const Versions = sequelize.define<VersionsInterface>(
    'Versions',
    {
        id: {
            type: DataTypes.INTEGER({ length: 10 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        type: DataTypes.INTEGER,
        version: DataTypes.STRING,
    },
    {
        tableName: 'versions',
        updatedAt: false,
        createdAt: false,
    },
);
